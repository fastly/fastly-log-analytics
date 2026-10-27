"""Standalone bounded source-object worker for high-scale ingest."""

from __future__ import annotations

import logging
import os
import random
import time
from argparse import ArgumentParser
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from threading import Event, Thread
from typing import TYPE_CHECKING, Any

from backend import config as app_config
from backend.core.clickhouse import ClickHouseClient
from backend.high_scale.archive import (
    ArchivePublication,
    ClickHouseBatchAdapter,
    ClickHousePublication,
    PostgresBatchManifestStore,
    S3ObjectStore,
    from_environment,
)
from backend.high_scale.postgres_control import (
    DeletionController,
    HighScaleArchiveExpirer,
    HighScaleDeletionSweeper,
    PostgresControlPlane,
    PostgresDeletionLedger,
)

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import (
        HighScaleWorkerCoordinator,
        PageRun,
    )

_worker_logger = logging.getLogger("backend.high_scale.worker")


@dataclass(frozen=True)
class WorkerPageResult:
    service_id: str
    domain: str
    page: PageRun | None
    error: Exception | None = None


class FairPageSelector:
    """Chooses which (service, domain) pages run each tick with weighted fairness."""

    def __init__(
        self,
        pairs: tuple[tuple[str, str], ...],
        *,
        capacity: int | None = None,
        weights: Mapping[str, int] | None = None,
    ) -> None:
        services = {service_id for service_id, _ in pairs}
        if capacity is not None and capacity < 0:
            raise ValueError("pages per tick must be non-negative")
        if capacity and capacity < len(services):
            raise ValueError("pages per tick must be at least the number of services")
        self._pairs = pairs
        self._order = {pair: index for index, pair in enumerate(pairs)}
        self._capacity = capacity or None
        self._services = tuple(sorted(services))
        self._weights: dict[str, int] = {}
        for service_id in self._services:
            w = (weights or {}).get(service_id, 1)
            if w <= 0:
                raise ValueError("service weight must be positive")
            self._weights[service_id] = w
        self._queues: dict[str, deque[str]] = {service_id: deque() for service_id in self._services}
        self._deficits: dict[str, int] = {service_id: 0 for service_id in self._services}
        self._queued: set[tuple[str, str]] = set()

    def select(self) -> list[tuple[str, str]]:
        for pair in self._pairs:
            if pair not in self._queued:
                self._queued.add(pair)
                self._queues[pair[0]].append(pair[1])
        capacity = self._capacity if self._capacity is not None else len(self._pairs)
        selected: list[tuple[str, str]] = []
        for service_id in self._services:
            if len(selected) >= capacity:
                break
            if self._queues[service_id]:
                selected.append((service_id, self._queues[service_id].popleft()))
                self._deficits[service_id] += self._weights[service_id] - 1
        while len(selected) < capacity and any(self._queues.values()):
            progressed = False
            for service_id in self._services:
                queue = self._queues[service_id]
                while queue and self._deficits[service_id] >= 1 and len(selected) < capacity:
                    selected.append((service_id, queue.popleft()))
                    self._deficits[service_id] -= 1
                    progressed = True
            if not progressed:
                for service_id, queue in self._queues.items():
                    if queue:
                        self._deficits[service_id] += self._weights[service_id]
        self._queued.difference_update(selected)
        selected.sort(key=self._order.__getitem__)
        if self._queued:
            _worker_logger.info(
                "high_scale.fair_dispatch",
                extra={"dispatched": len(selected), "deferred": len(self._queued), "capacity": capacity},
            )
        return selected


def _weights_from_env() -> dict[str, int]:
    weights: dict[str, int] = {}
    for entry in _csv_env("HIGH_SCALE_SERVICE_WEIGHTS"):
        service_id, sep, raw = entry.partition("=")
        if not sep or not service_id.strip() or not raw.strip().isdigit() or int(raw) <= 0:
            raise ValueError("HIGH_SCALE_SERVICE_WEIGHTS entries must look like service=positive_integer")
        weights[service_id.strip()] = int(raw)
    return weights


class HighScaleWorkerLoop:
    """Runs bounded source pages without coupling high-scale work to APScheduler."""

    def __init__(
        self,
        *,
        coordinator: HighScaleWorkerCoordinator | None = None,
        coordinators: tuple[tuple[str, HighScaleWorkerCoordinator], ...] | None = None,
        service_ids: tuple[str, ...] = (),
        domains: tuple[str, ...] = ("request", "rum", "cmcd"),
        interval_seconds: float = 5.0,
        stop_event: Event | None = None,
        sleeper: Any = time.sleep,
        deletion_sweeper: HighScaleDeletionSweeper | None = None,
        deletion_sweepers: tuple[tuple[str, HighScaleDeletionSweeper | None], ...] | None = None,
        deletion_sweep_limit: int = 100,
        pages_per_tick: int | None = None,
        service_weights: Mapping[str, int] | None = None,
    ) -> None:
        if coordinators is not None:
            resolved_coordinators = coordinators
            resolved_service_ids = tuple(service_id for service_id, _ in coordinators)
        else:
            if not service_ids:
                raise ValueError("at least one high-scale service is required")
            if coordinator is None:
                raise ValueError("coordinator is required")
            resolved_service_ids = service_ids
            resolved_coordinators = tuple((service_id, coordinator) for service_id in service_ids)
        if not resolved_service_ids:
            raise ValueError("at least one high-scale service is required")
        if not domains:
            raise ValueError("at least one high-scale domain is required")
        if interval_seconds < 0:
            raise ValueError("worker interval must be non-negative")
        self._coordinator = coordinator
        self._coordinators = resolved_coordinators
        self._service_ids = resolved_service_ids
        self._domains = domains
        self._interval_seconds = interval_seconds
        self._stop_event = stop_event or Event()
        self._sleeper = sleeper
        self._deletion_sweeper = deletion_sweeper
        if deletion_sweepers is not None:
            self._deletion_sweepers = deletion_sweepers
        else:
            self._deletion_sweepers = tuple((service_id, deletion_sweeper) for service_id in resolved_service_ids)
        self._deletion_sweep_limit = deletion_sweep_limit
        self._deletion_thread: Thread | None = None
        self._last_system_cleanup = 0.0
        pair_list = tuple((service_id, domain) for service_id in resolved_service_ids for domain in domains)
        self._selector = FairPageSelector(
            pair_list,
            capacity=pages_per_tick,
            weights=service_weights,
        )

    def run_once(self) -> tuple[WorkerPageResult, ...]:
        results: list[WorkerPageResult] = []
        coordinators = dict(self._coordinators)
        for service_id, domain in self._selector.select():
            try:
                page = coordinators[service_id].run_page(service_id=service_id, domain=domain)
            except Exception as exc:
                _worker_logger.exception(
                    "high-scale worker page failed", extra={"service_id": service_id, "domain": domain}
                )
                results.append(WorkerPageResult(service_id, domain, None, exc))
            else:
                results.append(WorkerPageResult(service_id, domain, page))
        self._run_deletion_sweeps()
        self._run_system_cleanups()
        return tuple(results)

    def _deletion_targets(self) -> tuple[tuple[str, HighScaleDeletionSweeper], ...]:
        return tuple((service_id, sweeper) for service_id, sweeper in self._deletion_sweepers if sweeper is not None)

    def _run_deletion_sweeps(self) -> None:
        targets = self._deletion_targets()
        if not targets or (self._deletion_thread is not None and self._deletion_thread.is_alive()):
            return
        self._deletion_thread = Thread(
            target=self._sweep_targets, args=(targets,), name="high-scale-deletion-sweep", daemon=True
        )
        self._deletion_thread.start()

    def _sweep_targets(self, targets: tuple[tuple[str, HighScaleDeletionSweeper], ...]) -> None:
        for service_id, sweeper in targets:
            try:
                sweeper.sweep(service_id=service_id, limit=self._deletion_sweep_limit)
            except Exception:
                _worker_logger.exception("high-scale deletion sweep failed", extra={"service_id": service_id})

    def wait_for_deletion_sweep(self, timeout: float | None = None) -> None:
        if self._deletion_thread is not None:
            self._deletion_thread.join(timeout)

    def _run_system_cleanups(self) -> None:
        now = time.time()
        if now - self._last_system_cleanup < 300:
            return
        self._last_system_cleanup = now
        for service_id, _ in self._coordinators:
            try:
                from backend.utils.telemetry import run_usage_log_cleanup

                run_usage_log_cleanup(service_id)
            except Exception:
                _worker_logger.debug("periodic usage log cleanup failed", extra={"service_id": service_id})
        try:
            from backend.core.clickhouse import get_clickhouse_client, maintain_clickhouse_system_tables

            client = get_clickhouse_client()
            if client is not None:
                maintain_clickhouse_system_tables(client)
        except Exception:
            _worker_logger.debug("periodic clickhouse system tables maintenance failed")

    def _has_full_page(self, results: tuple[WorkerPageResult, ...]) -> bool:
        if not self._coordinators:
            return False
        coord_map = dict(self._coordinators)
        for r in results:
            if r.page is not None:
                page_size = getattr(coord_map.get(r.service_id), "_page_size", None)
                if page_size is not None and page_size > 0 and r.page.discovered >= page_size:
                    return True
        return False

    def run(self, *, max_iterations: int | None = None) -> None:
        if max_iterations is not None and max_iterations < 0:
            raise ValueError("max_iterations must be non-negative")
        iterations = 0
        while not self._stop_event.is_set():
            results = self.run_once()
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                self.wait_for_deletion_sweep()
                return
            if self._interval_seconds > 0 and not self._has_full_page(results):
                self._sleeper(random.uniform(0.5, 1.0) * self._interval_seconds)

    def stop(self) -> None:
        self._stop_event.set()


def build_worker_from_environment() -> HighScaleWorkerLoop:
    """Build the durable FOS → archive → ClickHouse worker from process config."""
    from backend.core.duckdb import _get_fos_client, get_source_for_service
    from backend.high_scale.ingest_controller import (
        AggregateBatchAdapter,
        HighScaleIngestController,
        HighScaleWorkerCoordinator,
        S3SourceObjectLister,
        S3SourceObjectReader,
    )

    high_scale_config = from_environment()
    if not high_scale_config.enabled:
        raise RuntimeError("high-scale worker requires HIGH_SCALE_ENABLED=1")
    topology = high_scale_config.topology_profile()
    _worker_logger.info(
        "high_scale.topology_profile",
        extra={
            "profile": topology.name,
            "clickhouse_replicas_per_shard": topology.clickhouse_replicas_per_shard,
            "keeper_members": topology.keeper_members,
            "production_ha": topology.production_ha,
            "shard_count": topology.shard_count,
        },
    )
    service_ids = _csv_env("HIGH_SCALE_SERVICE_IDS")
    if not service_ids:
        service_ids = tuple(
            cfg["service_id"] for cfg in app_config.list_configs() if isinstance(cfg, dict) and cfg.get("service_id")
        )
    if not service_ids:
        raise ValueError("HIGH_SCALE_SERVICE_IDS is required (none found in configs/)")
    worker_id = (
        os.getenv("HIGH_SCALE_WORKER_ID", "").strip()
        or os.getenv("POD_NAME", "").strip()
        or f"high-scale-{os.getpid()}"
    )
    page_size = int(os.getenv("HIGH_SCALE_PAGE_SIZE", "100"))
    lease_seconds = float(os.getenv("HIGH_SCALE_LEASE_SECONDS", "300"))
    object_concurrency = int(os.getenv("HIGH_SCALE_OBJECT_CONCURRENCY", "4"))
    interval_seconds = float(os.getenv("HIGH_SCALE_WORKER_INTERVAL_SECONDS", "5"))
    domains = tuple(_csv_env("HIGH_SCALE_DOMAINS") or ("request", "rum_vitals", "rum_errors"))
    control = PostgresControlPlane()
    control.create_schema()
    initial_cursor = os.getenv("HIGH_SCALE_INITIAL_CURSOR", "__initial__")
    for service_id in service_ids:
        control.initialize_owner(service_id, owner="high_scale", source_cursor=initial_cursor)
    clickhouse_settings = app_config.load_clickhouse_config()
    if clickhouse_settings is None:
        raise RuntimeError("high-scale worker requires CLICKHOUSE_ENABLED=true")
    clickhouse = ClickHouseClient(clickhouse_settings)
    serving = ClickHousePublication(PostgresBatchManifestStore(control), ClickHouseBatchAdapter(clickhouse))
    aggregates_enabled = os.getenv("HIGH_SCALE_AGGREGATES_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
    if aggregates_enabled:
        aggregates = ClickHousePublication(PostgresBatchManifestStore(control), AggregateBatchAdapter(clickhouse))
    else:
        aggregates = None
    deletion_enabled = os.getenv("HIGH_SCALE_DELETION_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
    deletion_sweep_limit = int(os.getenv("HIGH_SCALE_DELETION_SWEEP_LIMIT", "100"))
    deletion_grace = int(os.getenv("HIGH_SCALE_DELETION_GRACE_SECONDS", "900"))
    archive_prefix = os.getenv("HIGH_SCALE_ARCHIVE_PREFIX", "high-scale/archive")
    coordinators: list[HighScaleWorkerCoordinator] = []
    sweepers: list[HighScaleDeletionSweeper | None] = []
    for service_id in service_ids:
        source = get_source_for_service(service_id)
        if source is None:
            raise ValueError(f"no configured FOS source for high-scale service {service_id}")
        fos_client = _get_fos_client(source)
        bucket = source.get("bucket")
        if not isinstance(bucket, str) or not bucket:
            raise ValueError(f"FOS bucket is missing for high-scale service {service_id}")
        archive = ArchivePublication(S3ObjectStore(fos_client, bucket=bucket, prefix=archive_prefix))
        controller = HighScaleIngestController(
            ownership=control,  # type: ignore[arg-type]
            ledger=control,  # type: ignore[arg-type]
            control_plane=control,
            archive=archive,
            serving=serving,
            aggregates=aggregates,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            deletion_grace_seconds=deletion_grace,
        )
        coordinators.append(
            HighScaleWorkerCoordinator(
                lister=S3SourceObjectLister(fos_client, bucket=bucket, prefix=str(source.get("prefix") or "")),
                reader=S3SourceObjectReader(fos_client, bucket=bucket),
                controller=controller,
                ownership=control,
                ledger=control,
                worker_id=worker_id,
                control_plane=control,
                page_size=page_size,
                lease_seconds=lease_seconds,
                object_concurrency=object_concurrency,
            )
        )
        if deletion_enabled:
            deletion_ledger = PostgresDeletionLedger(control)
            deletion_controller = DeletionController(
                S3ObjectStore(fos_client, bucket=bucket), archive, deletion_ledger, ownership=deletion_ledger
            )
            sweepers.append(
                HighScaleDeletionSweeper(
                    control=control,
                    controller=deletion_controller,
                    archive_expirer=HighScaleArchiveExpirer(control=control, archive=archive),
                )
            )
        else:
            sweepers.append(None)
    pages_per_tick = int(os.getenv("HIGH_SCALE_PAGES_PER_TICK", "0") or "0") or None
    return _MultiServiceWorkerLoop(
        coordinators=tuple(zip(service_ids, coordinators, strict=True)),
        deletion_sweepers=tuple(zip(service_ids, sweepers, strict=True)),
        deletion_sweep_limit=deletion_sweep_limit,
        domains=domains,
        interval_seconds=interval_seconds,
        pages_per_tick=pages_per_tick,
        service_weights=_weights_from_env(),
    )


_MultiServiceWorkerLoop = HighScaleWorkerLoop


def _csv_env(name: str) -> tuple[str, ...]:
    return tuple(value.strip() for value in os.getenv(name, "").split(",") if value.strip())


def main() -> int:
    if os.environ.get("OTEL_EXPORTER") == "prometheus":
        try:
            from prometheus_client import start_http_server

            logging.info("Starting Prometheus metrics server on port 8000")
            start_http_server(8000)
        except Exception as e:
            logging.warning(f"Failed to start prometheus_client: {e}")
    parser = ArgumentParser(description="Run the bounded high-scale source worker")
    parser.add_argument("--once", action="store_true", help="process one bounded page per service and domain")
    parser.add_argument("--iterations", type=int, default=None)
    args = parser.parse_args()
    build_worker_from_environment().run(max_iterations=1 if args.once else args.iterations)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
