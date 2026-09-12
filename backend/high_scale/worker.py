"""Continuous bounded worker loop for the high-scale source plane."""

from __future__ import annotations

import logging
import os
import time
from argparse import ArgumentParser
from dataclasses import dataclass
from threading import Event
from typing import Any

from backend import config as app_config
from backend.core.clickhouse_client import ClickHouseClient
from backend.core.duckdb import _get_fos_client, get_source_for_service
from backend.high_scale.archive_publication import ArchivePublication, S3ObjectStore
from backend.high_scale.config import from_environment
from backend.high_scale.ingest_controller import HighScaleIngestController
from backend.high_scale.orchestration import HighScaleWorkerCoordinator, PageRun
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.high_scale.publication import ClickHouseBatchAdapter, ClickHousePublication, InMemoryBatchManifestStore
from backend.high_scale.source_discovery import S3SourceObjectLister, S3SourceObjectReader

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerPageResult:
    service_id: str
    domain: str
    page: PageRun | None
    error: Exception | None = None


class HighScaleWorkerLoop:
    """Runs bounded source pages without coupling high-scale work to APScheduler."""

    def __init__(
        self,
        *,
        coordinator: HighScaleWorkerCoordinator,
        service_ids: tuple[str, ...],
        domains: tuple[str, ...] = ("request", "rum", "cmcd"),
        interval_seconds: float = 5.0,
        stop_event: Event | None = None,
        sleeper: Any = time.sleep,
    ) -> None:
        if not service_ids:
            raise ValueError("at least one high-scale service is required")
        if not domains:
            raise ValueError("at least one high-scale domain is required")
        if interval_seconds < 0:
            raise ValueError("worker interval must be non-negative")
        self._coordinator = coordinator
        self._service_ids = service_ids
        self._domains = domains
        self._interval_seconds = interval_seconds
        self._stop_event = stop_event or Event()
        self._sleeper = sleeper

    def run_once(self) -> tuple[WorkerPageResult, ...]:
        results: list[WorkerPageResult] = []
        for service_id in self._service_ids:
            for domain in self._domains:
                try:
                    page = self._coordinator.run_page(service_id=service_id, domain=domain)
                except Exception as exc:
                    logger.exception(
                        "high-scale worker page failed",
                        extra={"service_id": service_id, "domain": domain},
                    )
                    results.append(WorkerPageResult(service_id, domain, None, exc))
                else:
                    results.append(WorkerPageResult(service_id, domain, page))
        return tuple(results)

    def run(self, *, max_iterations: int | None = None) -> None:
        if max_iterations is not None and max_iterations < 0:
            raise ValueError("max_iterations must be non-negative")
        iterations = 0
        while not self._stop_event.is_set():
            self.run_once()
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                return
            if self._interval_seconds:
                self._sleeper(self._interval_seconds)

    def stop(self) -> None:
        self._stop_event.set()


def build_worker_from_environment() -> HighScaleWorkerLoop:
    """Build the durable FOS → archive → ClickHouse worker from process config."""

    high_scale_config = from_environment()
    if not high_scale_config.enabled:
        raise RuntimeError("high-scale worker requires HIGH_SCALE_ENABLED=1")
    service_ids = _csv_env("HIGH_SCALE_SERVICE_IDS")
    if not service_ids:
        raise ValueError("HIGH_SCALE_SERVICE_IDS is required")
    worker_id = os.getenv("HIGH_SCALE_WORKER_ID", "").strip() or f"high-scale-{os.getpid()}"
    page_size = int(os.getenv("HIGH_SCALE_PAGE_SIZE", "100"))
    lease_seconds = float(os.getenv("HIGH_SCALE_LEASE_SECONDS", "300"))
    interval_seconds = float(os.getenv("HIGH_SCALE_WORKER_INTERVAL_SECONDS", "5"))
    domains = tuple(_csv_env("HIGH_SCALE_DOMAINS") or ("request", "rum_vitals", "rum_errors"))

    control = PostgresControlPlane()
    control.create_schema()
    for service_id in service_ids:
        control.initialize_owner(
            service_id,
            owner="high_scale",
            source_cursor=os.getenv("HIGH_SCALE_INITIAL_CURSOR", "__initial__"),
        )

    clickhouse_settings = app_config.load_clickhouse_config()
    if clickhouse_settings is None:
        raise RuntimeError("high-scale worker requires CLICKHOUSE_ENABLED=true")
    clickhouse = ClickHouseClient(clickhouse_settings)
    serving = ClickHousePublication(InMemoryBatchManifestStore(), ClickHouseBatchAdapter(clickhouse))

    coordinators: list[HighScaleWorkerCoordinator] = []
    for service_id in service_ids:
        source = get_source_for_service(service_id)
        if source is None:
            raise ValueError(f"no configured FOS source for high-scale service {service_id}")
        fos_client = _get_fos_client(source)
        bucket = source.get("bucket")
        if not isinstance(bucket, str) or not bucket:
            raise ValueError(f"FOS bucket is missing for high-scale service {service_id}")
        archive_prefix = os.getenv("HIGH_SCALE_ARCHIVE_PREFIX", "high-scale/archive")
        archive = ArchivePublication(S3ObjectStore(fos_client, bucket=bucket, prefix=archive_prefix))
        controller = HighScaleIngestController(
            ownership=control,  # type: ignore[arg-type]
            ledger=control,  # type: ignore[arg-type]
            control_plane=control,
            archive=archive,
            serving=serving,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            deletion_grace_seconds=int(os.getenv("HIGH_SCALE_DELETION_GRACE_SECONDS", "900")),
        )
        coordinators.append(
            HighScaleWorkerCoordinator(
                lister=S3SourceObjectLister(
                    fos_client,
                    bucket=bucket,
                    prefix=str(source.get("prefix") or ""),
                ),
                reader=S3SourceObjectReader(fos_client, bucket=bucket),
                controller=controller,
                ownership=control,
                ledger=control,
                worker_id=worker_id,
                control_plane=control,
                page_size=page_size,
                lease_seconds=lease_seconds,
            )
        )

    return _MultiServiceWorkerLoop(
        coordinators=tuple(zip(service_ids, coordinators, strict=True)),
        domains=domains,
        interval_seconds=interval_seconds,
    )


class _MultiServiceWorkerLoop(HighScaleWorkerLoop):
    def __init__(
        self,
        *,
        coordinators: tuple[tuple[str, HighScaleWorkerCoordinator], ...],
        domains: tuple[str, ...],
        interval_seconds: float,
    ) -> None:
        self._coordinators = coordinators
        self._domains = domains
        self._interval_seconds = interval_seconds
        self._stop_event = Event()
        self._sleeper = time.sleep

    def run_once(self) -> tuple[WorkerPageResult, ...]:
        results: list[WorkerPageResult] = []
        for service_id, coordinator in self._coordinators:
            for domain in self._domains:
                try:
                    page = coordinator.run_page(service_id=service_id, domain=domain)
                except Exception as exc:
                    logger.exception(
                        "high-scale worker page failed",
                        extra={"service_id": service_id, "domain": domain},
                    )
                    results.append(WorkerPageResult(service_id, domain, None, exc))
                else:
                    results.append(WorkerPageResult(service_id, domain, page))
        return tuple(results)


def _csv_env(name: str) -> tuple[str, ...]:
    return tuple(value.strip() for value in os.getenv(name, "").split(",") if value.strip())


def main() -> None:
    parser = ArgumentParser(description="Run the bounded high-scale source worker")
    parser.add_argument("--once", action="store_true", help="process one bounded page per service and domain")
    parser.add_argument("--iterations", type=int, default=None)
    args = parser.parse_args()
    worker = build_worker_from_environment()
    worker.run(max_iterations=1 if args.once else args.iterations)


if __name__ == "__main__":
    main()
