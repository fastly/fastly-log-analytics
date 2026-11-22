"""Fast fake parity test for 6-paginated-object canonical ingest with interrupted publication."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pytest

from backend.high_scale.aggregate_query import AggregateRequest, query_clickhouse_aggregate
from backend.high_scale.archive import (
    ArchivePublication,
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InMemoryObjectStore,
    InsertReceipt,
    PublicationStatus,
    ServingWatermark,
    build_batch_id_from_manifest_id,
)
from backend.high_scale.ingest_controller import HighScaleIngestController, SourceObjectDescriptor
from tests.high_scale.conftest import HighScaleLedger, OwnershipStore

CANONICAL_FIXTURE = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "canonical_ingest.ndjson"


class _FaultyObjectStore(InMemoryObjectStore):
    def __init__(self) -> None:
        super().__init__()
        self.fail_suffix: str | None = None

    def put(self, key: str, payload: bytes) -> None:
        if self.fail_suffix and key.endswith(self.fail_suffix):
            raise RuntimeError(f"interrupted object store put for {key}")
        super().put(key, payload)


class _FaultyClickHouse:
    def __init__(self) -> None:
        self.batches: list[HighScaleBatch] = []
        self.fail_domain: str | None = None

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        if self.fail_domain and batch.domain == self.fail_domain:
            raise RuntimeError(f"interrupted ClickHouse insert for {batch.domain}")
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)


def _partition_canonical_into_six_objects() -> list[tuple[SourceObjectDescriptor, bytes]]:
    lines = CANONICAL_FIXTURE.read_text("utf-8").splitlines()
    assert len(lines) == 7
    groups = [
        lines[0:1],
        lines[1:2],
        lines[2:3],
        lines[3:4],
        lines[4:6],
        lines[6:7],
    ]
    objects: list[tuple[SourceObjectDescriptor, bytes]] = []
    for idx, group in enumerate(groups, start=1):
        payload = ("\n".join(group) + "\n").encode("utf-8")
        key = f"raw/request/canonical-page-{idx:02d}.ndjson"
        objects.append((SourceObjectDescriptor(key, f"sha256:canonical-{idx:02d}", len(payload), f"v{idx}"), payload))
    return objects


@pytest.mark.no_postgres
def test_canonical_fixture_six_paginated_objects_interrupted_publication_parity() -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    objects = _FaultyObjectStore()
    archive = ArchivePublication(objects)
    serving_manifests = InMemoryBatchManifestStore()
    clickhouse = _FaultyClickHouse()
    aggregates = _FaultyClickHouse()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")

    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=ledger,
        archive=archive,
        serving=ClickHousePublication(serving_manifests, clickhouse),
        aggregates=ClickHousePublication(InMemoryBatchManifestStore(), aggregates),
        worker_id="worker-canonical-parity",
        deletion_grace_seconds=60,
    )

    six_objects = _partition_canonical_into_six_objects()
    pages = [six_objects[0:2], six_objects[2:4], six_objects[4:6]]
    now = datetime(2027, 1, 15, 8, 2, tzinfo=UTC)

    # Page 1: interrupt at archive artifact upload, then recover.
    objects.fail_suffix = ".parquet"
    with pytest.raises(RuntimeError, match="interrupted object store put"):
        controller.ingest_batch(
            service_id="svc",
            domain="request",
            sources_payloads=pages[0],
            now=now,
            max_dead_letter_ratio=1.0,
        )
    assert len(clickhouse.batches) == 0
    objects.fail_suffix = None
    res1 = controller.ingest_batch(
        service_id="svc",
        domain="request",
        sources_payloads=pages[0],
        now=now,
        max_dead_letter_ratio=1.0,
    )
    assert (res1.total_events, res1.quarantined_events, res1.publication.rows_visible) == (2, 0, 2)

    # Page 2: interrupt at manifest write after artifact upload, then recover.
    objects.fail_suffix = ".json"
    with pytest.raises(RuntimeError, match="interrupted object store put"):
        controller.ingest_batch(
            service_id="svc",
            domain="request",
            sources_payloads=pages[1],
            now=now,
            max_dead_letter_ratio=1.0,
        )
    assert len(clickhouse.batches) == 1
    objects.fail_suffix = None
    res2 = controller.ingest_batch(
        service_id="svc",
        domain="request",
        sources_payloads=pages[1],
        now=now,
        max_dead_letter_ratio=1.0,
    )
    assert (res2.total_events, res2.quarantined_events, res2.publication.rows_visible) == (2, 0, 2)

    # Page 3: interrupt at serving insert after archive manifest commit, then recover.
    clickhouse.fail_domain = "request"
    with pytest.raises(RuntimeError, match="interrupted ClickHouse insert"):
        controller.ingest_batch(
            service_id="svc",
            domain="request",
            sources_payloads=pages[2],
            now=now,
            max_dead_letter_ratio=1.0,
        )
    assert len(clickhouse.batches) == 2
    assert any(
        m.status is PublicationStatus.PENDING
        for m in serving_manifests._manifests.values()  # noqa: SLF001
    )
    clickhouse.fail_domain = None
    res3 = controller.ingest_batch(
        service_id="svc",
        domain="request",
        sources_payloads=pages[2],
        now=now,
        max_dead_letter_ratio=1.0,
    )
    assert (res3.total_events, res3.quarantined_events, res3.publication.rows_visible) == (1, 2, 1)
    assert archive.is_replayable(res3.manifest.manifest_id)
    assert serving_manifests.get(build_batch_id_from_manifest_id(res3.manifest.manifest_id)).status is (
        PublicationStatus.VISIBLE
    )

    # Replay all 3 pages idempotently: zero new batches or rows.
    for page in pages:
        replay = controller.ingest_batch(
            service_id="svc",
            domain="request",
            sources_payloads=page,
            now=now,
            max_dead_letter_ratio=1.0,
        )
        assert replay.publication.duplicate is True
    assert len(clickhouse.batches) == 3

    # Exact visible row count, accepted/quarantined counts, and aggregate equality.
    visible_rows = [row for batch in clickhouse.batches for row in batch.rows]
    assert len(visible_rows) == 5
    assert sum(r.total_events for r in (res1, res2, res3)) == 5
    assert sum(r.quarantined_events for r in (res1, res2, res3)) == 2

    status_counts = Counter(row.get("status") for row in visible_rows)
    assert status_counts == Counter({200: 2, 204: 1, 503: 1, None: 1})
    count_5xx = sum(1 for row in visible_rows if isinstance(row.get("status"), int) and 500 <= row["status"] <= 599)
    assert count_5xx == 1

    distinct_ips = {row.get("client_ip") for row in visible_rows if row.get("client_ip")}
    assert len(distinct_ips) == 0

    total_bytes = sum(int(row.get("resp_bytes") or 0) for row in visible_rows)
    assert total_bytes == 70

    aggregate_rows = [row for batch in aggregates.batches if batch.domain == "request_aggregate" for row in batch.rows]

    class _FixtureAggregateQuery:
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            dim = (params or {}).get("dimension")
            rows = [row for row in aggregate_rows if row["dimension"] == dim]
            if "GROUP BY value" in sql:
                return [
                    {"value": value, "aggregate_count": count}
                    for value, count in sorted(
                        Counter(row["value"] for row in rows for _ in range(row["count"])).items(),
                        key=lambda pair: (-pair[1], pair[0]),
                    )
                ]
            return [{"total_count": sum(row["count"] for row in rows)}]

    watermark = ServingWatermark(
        "svc",
        "request",
        1,
        datetime(2027, 1, 15, 8, 0, tzinfo=UTC),
        datetime(2027, 1, 15, 8, 1, tzinfo=UTC),
        "cursor",
        "event",
        "event",
        True,
    )
    url_agg = query_clickhouse_aggregate(
        _FixtureAggregateQuery(),
        AggregateRequest("svc", "request", dimension="url"),
        watermark=watermark,
        now=datetime(2027, 1, 15, 8, 2, 1, tzinfo=UTC),
    )
    assert url_agg.request_count == 5
    assert url_agg.top_values == (("/dup", 2), ("/boundary", 1), ("/null", 1), ("/numeric", 1))

    status_agg = query_clickhouse_aggregate(
        _FixtureAggregateQuery(),
        AggregateRequest("svc", "request", dimension="status"),
        watermark=watermark,
        now=datetime(2027, 1, 15, 8, 2, 1, tzinfo=UTC),
    )
    assert dict(status_agg.top_values) == {"200": 2, "204": 1, "503": 1, "None": 1}

    ip_agg = query_clickhouse_aggregate(
        _FixtureAggregateQuery(),
        AggregateRequest("svc", "request", dimension="client_ip"),
        watermark=watermark,
        now=datetime(2027, 1, 15, 8, 2, 1, tzinfo=UTC),
    )
    assert len([ip for ip, _ in ip_agg.top_values if ip]) == 0
