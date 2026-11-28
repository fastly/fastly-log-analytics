"""P1 performance, bounded-scan, ingest-lag/drain/recovery, and query-latency gates."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pytest
from psycopg_pool import ConnectionPool
from testcontainers.postgres import PostgresContainer

from backend.config import ClickHouseConfig
from backend.core.clickhouse import ClickHouseClient
from backend.high_scale.archive import (
    ArchivePublication,
    ClickHouseBatchAdapter,
    ClickHousePublication,
    InMemoryObjectStore,
    PostgresBatchManifestStore,
    install_clickhouse_schema,
)
from backend.high_scale.dashboard import dashboard_aggregates
from backend.high_scale.ingest_controller import (
    HIGH_SCALE_DEFAULT_MAX_BATCH_BYTES,
    HIGH_SCALE_DEFAULT_MAX_BATCH_OBJECTS,
    HIGH_SCALE_PAGE_MAX_BYTES,
    HIGH_SCALE_SNAPSHOT_DEFAULT_MAX_BYTES,
    HIGH_SCALE_SNAPSHOT_DEFAULT_MAX_OBJECTS,
    AggregateBatchAdapter,
    HighScaleIngestController,
    HighScaleServiceRegistry,
    SourceObjectDescriptor,
    register_high_scale_services,
)
from backend.high_scale.insights.sql_queries import _pk_range_windows, build_pk_time_sample_clause
from backend.high_scale.network import _network_time_sample_clause, network_health
from backend.high_scale.origin import origin_aggregates
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.high_scale.recovery import (
    RecoveryCapacityMeasurement,
    evaluate_recovery_capacity,
    rebuild_serving_state_from_fos,
)
from backend.high_scale.rum import _rum_pk_sample_clauses
from backend.high_scale.security import security_aggregates
from backend.models.analytics import (
    AggregatesRequest,
    NetworkHealthRequest,
    OriginAggregatesRequest,
    SecurityAggregatesRequest,
)
from tests.high_scale.conftest import free_loopback_port, wait_for_clickhouse_ready

pytestmark = pytest.mark.no_postgres

_RANGE_RE = re.compile(
    r"event_timestamp >= toDateTime\((\d+), 'UTC'\) AND event_timestamp < toDateTime\((\d+), 'UTC'\)"
)
_CLICKHOUSE_IMAGE = os.environ.get("RUM_CLICKHOUSE_TEST_IMAGE", "clickhouse/clickhouse-server:25.8.4.13")


def _parse_windows(clause: str) -> list[tuple[int, int]]:
    return [(int(a), int(b)) for a, b in _RANGE_RE.findall(clause)]


def test_pr_deterministic_query_work_and_backlog_chunk_bounds() -> None:
    """PR gate: verify bounded PK scan windows, backlog chunk caps, and recovery capacity math."""
    end = datetime(2026, 11, 27, 12, 0, tzinfo=UTC)
    end_ts = int(end.timestamp())

    for span_days, stride in ((7, 8), (14, 8), (30, 8)):
        start_ts = end_ts - span_days * 86_400
        windows = _pk_range_windows(start_ts, end_ts, stride)
        assert 1 <= len(windows) <= 16
        assert all(start_ts <= a < b <= end_ts for a, b in windows)
        assert all(windows[i][1] <= windows[i + 1][0] for i in range(len(windows) - 1))

    combined = build_pk_time_sample_clause(
        b_stride=8,
        w_stride=4,
        baseline_start=end - timedelta(days=14),
        window_start=end - timedelta(hours=24),
        window_end=end,
    )
    assert len(_parse_windows(combined)) <= 22

    for span_days, stride in ((1, 20), (7, 100), (14, 500), (30, 1000)):
        clause = _network_time_sample_clause(stride, start=end - timedelta(days=span_days), end=end)
        windows = _parse_windows(clause)
        assert 1 <= len(windows) <= 16
        assert all(b - a >= 120 for a, b in windows)
        assert "line_ordinal" not in clause

    hourly_30d = _network_time_sample_clause(1000, bucket_seconds=3600, start=end - timedelta(days=30), end=end)
    assert hourly_30d.count("toDateTime(") == 720

    rum_clause, req_clause = _rum_pk_sample_clauses(16, start=end - timedelta(days=30), end=end)
    span_s = 30 * 86_400
    assert sum(b - a for a, b in _parse_windows(rum_clause)) / span_s < 0.07
    assert sum(b - a for a, b in _parse_windows(req_clause)) / span_s < 0.10

    assert HIGH_SCALE_PAGE_MAX_BYTES == 64 * 1024 * 1024
    assert HIGH_SCALE_DEFAULT_MAX_BATCH_OBJECTS == 50
    assert HIGH_SCALE_DEFAULT_MAX_BATCH_BYTES == 6 * 1024 * 1024
    assert HIGH_SCALE_SNAPSHOT_DEFAULT_MAX_OBJECTS == 500
    assert HIGH_SCALE_SNAPSHOT_DEFAULT_MAX_BYTES == 32 * 1024 * 1024

    feasible = evaluate_recovery_capacity(
        RecoveryCapacityMeasurement(
            events_per_second=1_000,
            recovery_window_seconds=21_600,
            archive_read_events_per_second=20_000,
            decode_events_per_second=20_000,
            aggregate_events_per_second=20_000,
            clickhouse_insert_events_per_second=60_000,
            replication_factor=3,
            live_ingest_reservation=0.25,
        )
    )
    assert feasible.feasible is True

    infeasible = evaluate_recovery_capacity(
        RecoveryCapacityMeasurement(
            events_per_second=5_000,
            recovery_window_seconds=3_600,
            archive_read_events_per_second=200_000,
            decode_events_per_second=200_000,
            aggregate_events_per_second=200_000,
            clickhouse_insert_events_per_second=150_000,
            replication_factor=3,
            live_ingest_reservation=0.25,
        )
    )
    assert infeasible.feasible is False
    assert infeasible.limiting_stage == "clickhouse_insert"


@contextmanager
def _colima_docker_env() -> Iterator[None]:
    previous = os.environ.get("DOCKER_HOST"), os.environ.get("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE")
    colima_socket = Path.home() / ".colima/default/docker.sock"
    if previous[0] is None and colima_socket.exists():
        os.environ["DOCKER_HOST"] = f"unix://{colima_socket}"
    if str(colima_socket) in os.environ.get("DOCKER_HOST", "") and previous[1] is None:
        os.environ["TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"] = "/var/run/docker.sock"
    try:
        yield
    finally:
        for name, value in zip(
            ("DOCKER_HOST", "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"),
            previous,
            strict=True,
        ):
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


@contextmanager
def _disposable_postgres_dsn() -> Iterator[str]:
    external = os.environ.get("DISPOSABLE_POSTGRES_DSN") or os.environ.get("TEST_METADATA_DSN")
    if external:
        yield external
        return
    with _colima_docker_env():
        with PostgresContainer("postgres:16-alpine", driver="psycopg") as postgres:
            yield postgres.get_connection_url().replace("postgresql+psycopg://", "postgresql://", 1)


def _ch_config_from_url(url: str, database: str) -> ClickHouseConfig:
    parsed = urlsplit(url)
    secure = parsed.scheme == "https"
    return ClickHouseConfig(
        host=parsed.hostname or "127.0.0.1",
        port=parsed.port or (8443 if secure else 8123),
        database=database,
        user=parsed.username or "admin",
        password=parsed.password or "",
        secure=secure,
    )


@contextmanager
def _disposable_clickhouse_client() -> Iterator[ClickHouseClient]:
    external = os.environ.get("DISPOSABLE_CLICKHOUSE_HTTP_URL")
    db_name = f"fla_p1_gate_{uuid4().hex[:8]}"
    container_name: str | None = None
    if external:
        parsed = urlsplit(external)
        base = f"{parsed.scheme}://{parsed.hostname}:{parsed.port or (8443 if parsed.scheme == 'https' else 8123)}/"
        auth = (parsed.username or "admin", parsed.password or "") if (parsed.username or parsed.password) else None
    else:
        port = free_loopback_port()
        container_name = f"fla-p1-gate-ch-{uuid4().hex[:8]}"
        with _colima_docker_env():
            subprocess.run(
                [
                    "docker",
                    "run",
                    "-d",
                    "--rm",
                    "--name",
                    container_name,
                    "-p",
                    f"127.0.0.1:{port}:8123",
                    "-e",
                    "CLICKHOUSE_USER=admin",
                    "-e",
                    "CLICKHOUSE_PASSWORD=adminpw",
                    "-e",
                    "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
                    _CLICKHOUSE_IMAGE,
                ],
                check=True,
                capture_output=True,
            )
        base = f"http://127.0.0.1:{port}/"
        auth = ("admin", "adminpw")
    try:
        wait_for_clickhouse_ready(base, timeout_s=45.0)
        httpx.post(base, content=f"CREATE DATABASE IF NOT EXISTS {db_name}", auth=auth, timeout=15.0).raise_for_status()
        cfg_url = external if external else base.replace("http://", "http://admin:adminpw@", 1)
        client = ClickHouseClient(_ch_config_from_url(cfg_url, db_name))
        client._enable_cf_rewrite_in_tests = True  # type: ignore[attr-defined]
        try:
            install_clickhouse_schema(client)
            yield client
        finally:
            try:
                httpx.post(base, content=f"DROP DATABASE IF EXISTS {db_name}", auth=auth, timeout=15.0)
            finally:
                client.close()
    finally:
        if container_name is not None:
            with _colima_docker_env():
                subprocess.run(["docker", "rm", "-f", container_name], check=False, capture_output=True)


class _CountingClickHouseClient:
    """Forwards calls to ClickHouseClient while recording SQL query count per endpoint."""

    def __init__(self, inner: ClickHouseClient) -> None:
        self._inner = inner
        self._http = inner._http
        self.count = 0

    def reset_count(self) -> None:
        self.count = 0

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        settings: Any = None,
        rewrite_cf: bool = True,
    ) -> list[dict[str, Any]]:
        self.count += 1
        return self._inner.execute(sql, params, settings=settings, rewrite_cf=rewrite_cf)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _sample_gz_batch(
    service_id: str, start: datetime, batch_idx: int, rows_per_batch: int
) -> tuple[SourceObjectDescriptor, bytes]:
    lines: list[bytes] = []
    pops = ("IAD", "SJC", "LHR", "FRA")
    countries = ("US", "GB", "DE", "CA")
    statuses = (200, 200, 200, 304, 404, 502)
    urls = ("/index.html", "/api/v1/items", "/assets/app.js", "/checkout")
    for idx in range(rows_per_batch):
        ts = start + timedelta(seconds=(batch_idx * rows_per_batch + idx) % 3600)
        row = {
            "service_id": service_id,
            "timestamp": ts.isoformat(),
            "status": statuses[idx % len(statuses)],
            "method": "GET" if idx % 5 else "POST",
            "url": urls[idx % len(urls)],
            "host": "edge.example.com",
            "pop": pops[idx % len(pops)],
            "country": countries[idx % len(countries)],
            "city": "Ashburn",
            "region": "VA",
            "client_ip": f"198.51.100.{(idx % 200) + 1}",
            "client_as_number": 13335 + (idx % 4),
            "cache": "HIT" if idx % 3 else "MISS",
            "origin_fetch": (idx % 3) == 0,
            "origin_latency": 25 + (idx % 40),
            "ttfb": 12 + (idx % 25),
            "ttlb": 30 + (idx % 50),
            "elapsed": 30_000 + (idx % 20_000),
            "rtt": 18_000 + (idx % 10_000),
            "bytes_sent": 1024 + (idx % 4096),
            "tls_client_ja3": f"ja3-fp-{idx % 6}",
            "ua": "Mozilla/5.0",
        }
        lines.append((json.dumps(row) + "\n").encode("utf-8"))
    raw_bytes = b"".join(lines)
    gz_bytes = gzip.compress(raw_bytes, compresslevel=1)
    digest = f"sha256:{hashlib.sha256(gz_bytes).hexdigest()}"
    key = f"raw/request/2026/11/27/12/batch-{batch_idx:03d}.log.gz"
    return SourceObjectDescriptor(key, digest, len(gz_bytes), f"v{batch_idx}"), gz_bytes


def test_disposable_ingest_lag_drain_recovery_and_query_latency_budgets() -> None:
    """Disposable PG + CH gate: multi-batch ingest lag, idempotent FOS recovery, and bounded query latency."""
    if os.environ.get("RUN_DISPOSABLE_PERF_RECOVERY_GATE") != "1":
        pytest.skip("set RUN_DISPOSABLE_PERF_RECOVERY_GATE=1 to run the disposable perf/recovery gate")

    with _disposable_postgres_dsn() as pg_dsn, _disposable_clickhouse_client() as ch_client:
        pool = ConnectionPool(conninfo=pg_dsn, min_size=1, max_size=4, timeout=30, open=False)
        pool.open(wait=True)
        try:
            control = PostgresControlPlane(pool=pool, pool_timeout=30)
            control.create_schema()

            service_id = f"p1-gate-{uuid4().hex[:10]}"
            owner = control.initialize_owner(service_id, owner="high_scale", source_cursor="initial")

            objects = InMemoryObjectStore()
            archive = ArchivePublication(objects)
            manifest_store = PostgresBatchManifestStore(control)
            serving = ClickHousePublication(manifest_store, ClickHouseBatchAdapter(ch_client))
            aggregates = ClickHousePublication(manifest_store, AggregateBatchAdapter(ch_client))
            controller = HighScaleIngestController(
                control_plane=control,
                archive=archive,
                serving=serving,
                aggregates=aggregates,
                worker_id="worker-p1-gate",
                deletion_grace_seconds=60,
            )

            end = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=2)
            start = end - timedelta(hours=2)
            total_batches = 6
            rows_per_batch = 1_000
            total_expected = total_batches * rows_per_batch

            batches = [_sample_gz_batch(service_id, start, idx, rows_per_batch) for idx in range(total_batches)]
            descriptors = [desc for desc, _ in batches]
            control.discover_sources_batch(
                service_id,
                "request",
                descriptors,
                expected_owner="high_scale",
                expected_owner_epoch=owner.owner_epoch,
            )
            claimed = control.claim_sources_batch(
                service_id,
                [d.object_key for d in descriptors],
                "worker-p1-gate",
                expected_owner="high_scale",
                expected_owner_epoch=owner.owner_epoch,
            )
            claim_tokens = [item.claim_token for item in claimed]

            ingest_started = time.perf_counter()
            result = controller.ingest_batch(
                service_id=service_id,
                domain="request",
                sources_payloads=batches,
                claim_tokens=claim_tokens,
                now=end + timedelta(seconds=30),
                max_dead_letter_ratio=0.0,
            )
            ingest_elapsed = time.perf_counter() - ingest_started
            assert ingest_elapsed < 30.0, f"ingest took {ingest_elapsed:.2f}s"
            assert result.total_events == total_expected
            assert result.quarantined_events == 0

            row_count = ch_client.execute(
                "SELECT count() AS n FROM request_facts WHERE service_id = {sid:String}",
                {"sid": service_id},
            )[0]["n"]
            assert int(row_count) == total_expected

            # Simulate serving-store loss while Postgres manifests + FOS archive remain intact.
            for table in (
                "request_facts",
                "request_aggregates",
                "request_aggregates_1h",
                "request_insight_hourly",
                "origin_minute_dimensions",
                "performance_minute_dimensions",
                "network_minute_dimensions",
                "security_minute_dimensions",
                "high_scale_batch_publications",
            ):
                ch_client.execute(
                    f"ALTER TABLE {table} DELETE WHERE service_id = {{sid:String}} SETTINGS mutations_sync = 1",
                    {"sid": service_id},
                )
            wiped = ch_client.execute(
                "SELECT count() AS n FROM request_facts WHERE service_id = {sid:String}",
                {"sid": service_id},
            )[0]["n"]
            assert int(wiped) == 0

            recovery_capacity = RecoveryCapacityMeasurement(
                events_per_second=1_000,
                recovery_window_seconds=21_600,
                archive_read_events_per_second=20_000,
                decode_events_per_second=20_000,
                aggregate_events_per_second=20_000,
                clickhouse_insert_events_per_second=60_000,
                replication_factor=3,
                live_ingest_reservation=0.25,
            )
            rebuild_started = time.perf_counter()
            report = rebuild_serving_state_from_fos(
                control,
                archive,
                serving,
                service_id=service_id,
                domain="request",
                now=end + timedelta(seconds=60),
                recent_window=timedelta(hours=24),
                warm_window=timedelta(days=7),
                rebuild_id="p1-rebuild-gate",
                capacity=recovery_capacity,
                aggregates_publication=aggregates,
            )
            rebuild_elapsed = time.perf_counter() - rebuild_started
            assert rebuild_elapsed < 30.0, f"recovery took {rebuild_elapsed:.2f}s"
            assert report.full_rebuild_complete is True
            assert report.recovery_claim_valid is True
            assert report.errors == ()
            assert report.rows_replayed == total_expected

            # Idempotent re-run with identical rebuild_id produces the exact same row count.
            repeat_report = rebuild_serving_state_from_fos(
                control,
                archive,
                serving,
                service_id=service_id,
                domain="request",
                now=end + timedelta(seconds=90),
                recent_window=timedelta(hours=24),
                warm_window=timedelta(days=7),
                rebuild_id="p1-rebuild-gate",
                capacity=recovery_capacity,
                aggregates_publication=aggregates,
            )
            assert repeat_report.full_rebuild_complete is True
            restored_count = ch_client.execute(
                "SELECT count() AS n FROM request_facts WHERE service_id = {sid:String}",
                {"sid": service_id},
            )[0]["n"]
            assert int(restored_count) == total_expected

            spy_client = _CountingClickHouseClient(ch_client)
            registry = HighScaleServiceRegistry()
            register_high_scale_services(
                registry,
                client=spy_client,  # type: ignore[arg-type]
                service_ids=(service_id,),
                cursor_secret=b"p1-gate-cursor-secret-32-bytes!!",
                manifest_catalog=control,
                archive_for=lambda _sid: archive,
            )
            svc = registry.get(service_id)
            assert svc is not None

            start_iso = (start - timedelta(minutes=5)).isoformat()
            end_iso = (end + timedelta(minutes=5)).isoformat()

            for label, fn in (
                (
                    "dashboard_aggregates",
                    lambda: dashboard_aggregates(
                        svc,
                        AggregatesRequest(chart_interval="1 hour", chart_metric="requests", filters={}),
                        start_iso,
                        end_iso,
                    ),
                ),
                (
                    "security_aggregates",
                    lambda: security_aggregates(
                        svc,
                        SecurityAggregatesRequest(bucket_seconds=3600, filters={}),
                        start_iso,
                        end_iso,
                    ),
                ),
                (
                    "origin_aggregates",
                    lambda: origin_aggregates(
                        svc,
                        OriginAggregatesRequest(
                            bucket_minutes=60,
                            slow_urls_min_requests=1,
                            filters={},
                        ),
                        start_iso,
                        end_iso,
                        sections={"summary", "timeseries", "slow_urls"},
                    ),
                ),
                (
                    "network_health",
                    lambda: network_health(
                        svc,
                        NetworkHealthRequest(bucket_seconds=3600, top_n=10, filters={}),
                        start_iso,
                        end_iso,
                    ),
                ),
            ):
                spy_client.reset_count()
                t0 = time.perf_counter()
                resp = fn()
                dt = time.perf_counter() - t0
                assert dt < 5.0, f"{label} exceeded 5s latency budget ({dt:.2f}s)"
                assert spy_client.count <= 12, f"{label} issued {spy_client.count} ClickHouse queries"
                assert resp is not None
        finally:
            pool.close()
