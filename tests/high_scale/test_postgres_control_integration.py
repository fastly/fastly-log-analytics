"""Disposable-Postgres proof for High-Scale control-plane fences."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from psycopg_pool import ConnectionPool
from testcontainers.postgres import PostgresContainer

from backend.high_scale._common import LEGACY_ROLLUP_BATCH_UUID, visible_rollup_publication_fence_sql
from backend.high_scale.archive import (
    ArchiveArtifact,
    ArchiveManifest,
    ArchiveSourceObject,
    ArchiveState,
    ClickHousePublication,
    HighScaleBatch,
    InsertReceipt,
    PostgresBatchManifestStore,
    PublicationStatus,
)
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.high_scale.postgres_control.control_plane import SourceClaimToken, SourceObjectRecord

pytestmark = [
    pytest.mark.no_postgres,
    pytest.mark.postgres_integration,
]

_RUN_DISPOSABLE_POSTGRES = "RUN_DISPOSABLE_POSTGRES_INTEGRATION"


@pytest.fixture(scope="module")
def disposable_postgres_dsn() -> Iterator[str]:
    if os.environ.get(_RUN_DISPOSABLE_POSTGRES) != "1":
        pytest.skip(f"set {_RUN_DISPOSABLE_POSTGRES}=1 to run the disposable-Postgres proof")
    previous = os.environ.get("DOCKER_HOST"), os.environ.get("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE")
    colima_socket = Path.home() / ".colima/default/docker.sock"
    if previous[0] is None and colima_socket.exists():
        os.environ["DOCKER_HOST"] = f"unix://{colima_socket}"
    if str(colima_socket) in os.environ.get("DOCKER_HOST", "") and previous[1] is None:
        os.environ["TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"] = "/var/run/docker.sock"
    try:
        with PostgresContainer("postgres:16-alpine", driver="psycopg") as postgres:
            yield postgres.get_connection_url().replace("postgresql+psycopg://", "postgresql://", 1)
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


@pytest.fixture(scope="module")
def control(disposable_postgres_dsn: str) -> Iterator[PostgresControlPlane]:
    pool = ConnectionPool(conninfo=disposable_postgres_dsn, min_size=1, max_size=2, timeout=30, open=False)
    try:
        pool.open(wait=True)
        instance = PostgresControlPlane(pool=pool, pool_timeout=30)
        instance.create_schema()
        yield instance
    finally:
        pool.close()


def _initialize(control: PostgresControlPlane) -> tuple[str, int]:
    service_id = f"postgres-proof-{uuid4()}"
    owner = control.initialize_owner(service_id, owner="high_scale", source_cursor="initial")
    return service_id, owner.owner_epoch


def _discover_claim(
    control: PostgresControlPlane,
    service_id: str,
    owner_epoch: int,
    object_key: str,
) -> SourceObjectRecord:
    control.discover_source(
        service_id,
        "request",
        object_key,
        f"sha256:{object_key}",
        size_bytes=10,
        version="v1",
        expected_owner="high_scale",
        expected_owner_epoch=owner_epoch,
    )
    return control.claim_sources_batch(
        service_id,
        [object_key],
        "worker-current",
        expected_owner="high_scale",
        expected_owner_epoch=owner_epoch,
    )[0]


def _committed_manifest(control: PostgresControlPlane, service_id: str, owner_epoch: int) -> ArchiveManifest:
    now = datetime(2026, 11, 21, 12, tzinfo=UTC)
    source = ArchiveSourceObject(service_id, "request", "raw/bind.log", "sha256:raw/bind.log", 10, "v1")
    artifact = ArchiveArtifact(
        f"s3://disposable/{uuid4()}.parquet",
        "sha256:artifact",
        100,
        1,
        10,
        "sha256:canonical",
        "archive.v1",
        "normalize.v1",
    )
    manifest = ArchiveManifest(
        f"manifest-{uuid4()}",
        source,
        artifact,
        now,
        now + timedelta(minutes=1),
        now + timedelta(days=1),
        now + timedelta(days=2),
        1,
    )
    control.register_archive_manifest(
        manifest,
        owner_epoch=owner_epoch,
        state=ArchiveState.MANIFEST_COMMITTED,
    )
    return manifest


def _source_state(source: SourceObjectRecord) -> tuple[Any, ...]:
    return (
        source.status,
        source.owner,
        source.lease_until,
        source.lease_generation,
        source.archive_manifest_id,
        source.owner_epoch,
        source.last_error,
    )


def test_real_postgres_stale_claim_fences_affect_zero_rows(control: PostgresControlPlane) -> None:
    service_id, owner_epoch = _initialize(control)
    source = control.discover_source(
        service_id,
        "request",
        "raw/claim.log",
        "sha256:claim",
        expected_owner="high_scale",
        expected_owner_epoch=owner_epoch,
    )
    before = _source_state(source)

    attempts = (
        {
            "worker_id": "worker-current",
            "expected_owner": "high_scale-stale",
            "expected_owner_epoch": owner_epoch,
        },
        {
            "worker_id": "worker-current",
            "expected_owner": "high_scale",
            "expected_owner_epoch": owner_epoch + 1,
        },
    )
    for attempt in attempts:
        with pytest.raises((RuntimeError, ValueError)):
            control.claim_sources_batch(service_id, [source.object_key], **attempt)
        assert _source_state(control.source(service_id, source.object_key)) == before

    claimed = control.claim_sources_batch(
        service_id,
        [source.object_key],
        "worker-current",
        expected_owner="high_scale",
        expected_owner_epoch=owner_epoch,
    )
    assert len(claimed) == 1 and claimed[0].status == "claimed"
    assert claimed[0].lease_generation == source.lease_generation + 1


@pytest.mark.parametrize("operation", ["ack", "fail", "bind"])
def test_real_postgres_stale_transition_tokens_affect_zero_rows(
    control: PostgresControlPlane,
    operation: str,
) -> None:
    service_id, owner_epoch = _initialize(control)
    source = _discover_claim(control, service_id, owner_epoch, f"raw/{operation}.log")
    token = source.claim_token
    manifest = _committed_manifest(control, service_id, owner_epoch)
    before = _source_state(source)

    attempts = (
        (replace(token, owner="worker-stale"), owner_epoch),
        (replace(token, owner_epoch=owner_epoch + 1), owner_epoch),
        (replace(token, lease_generation=token.lease_generation + 1), owner_epoch),
        (token, owner_epoch + 1),
    )

    def transition(claim_token: SourceClaimToken, expected_epoch: int) -> None:
        common = {
            "expected_owner_epoch": expected_epoch,
            "claim_tokens": (claim_token,),
        }
        if operation == "ack":
            control.acknowledge_sources_batch(
                service_id,
                [source.object_key],
                manifest_id=manifest.manifest_id,
                **common,
            )
        elif operation == "fail":
            control.fail_sources_batch(
                service_id,
                [source.object_key],
                error="transient failure",
                **common,
            )
        else:
            control.bind_sources_to_archive_manifest(
                service_id,
                [source.object_key],
                manifest_id=manifest.manifest_id,
                error="publication failed",
                **common,
            )

    for stale_token, stale_epoch in attempts:
        with pytest.raises((RuntimeError, ValueError), match="stale|epoch"):
            transition(stale_token, stale_epoch)
        assert _source_state(control.source(service_id, source.object_key)) == before

    transition(token, owner_epoch)
    after = control.source(service_id, source.object_key)
    expected_status = {"ack": "acknowledged", "fail": "discovered", "bind": "archived"}[operation]
    assert after.status == expected_status


@pytest.mark.parametrize("operation", ["ack", "fail", "bind"])
def test_real_postgres_mixed_valid_and_stale_tokens_roll_back_the_whole_batch(
    control: PostgresControlPlane,
    operation: str,
) -> None:
    service_id, owner_epoch = _initialize(control)
    valid = _discover_claim(control, service_id, owner_epoch, f"raw/{operation}-valid.log")
    stale = _discover_claim(control, service_id, owner_epoch, f"raw/{operation}-stale.log")
    manifest = _committed_manifest(control, service_id, owner_epoch)
    before = {source.object_key: _source_state(source) for source in (valid, stale)}
    object_keys = [valid.object_key, stale.object_key]
    tokens = (valid.claim_token, replace(stale.claim_token, lease_generation=stale.lease_generation + 1))
    common = {"expected_owner_epoch": owner_epoch, "claim_tokens": tokens}

    with pytest.raises(RuntimeError, match="stale or missing claim"):
        if operation == "ack":
            control.acknowledge_sources_batch(
                service_id,
                object_keys,
                manifest_id=manifest.manifest_id,
                **common,
            )
        elif operation == "fail":
            control.fail_sources_batch(
                service_id,
                object_keys,
                error="transient failure",
                **common,
            )
        else:
            control.bind_sources_to_archive_manifest(
                service_id,
                object_keys,
                manifest_id=manifest.manifest_id,
                error="publication failed",
                **common,
            )

    assert {key: _source_state(control.source(service_id, key)) for key in object_keys} == before


class _ReceiptClient:
    def __init__(self, receipt: InsertReceipt) -> None:
        self.receipt = receipt
        self.calls = 0

    def insert(self, _batch: HighScaleBatch) -> InsertReceipt:
        self.calls += 1
        return self.receipt


@pytest.mark.parametrize(
    ("receipt", "error"),
    [
        (lambda batch: InsertReceipt("wrong-batch", len(batch.rows), batch.digest), "batch id mismatch"),
        (lambda batch: InsertReceipt(batch.batch_id, len(batch.rows), "sha256:wrong"), "digest mismatch"),
        (lambda batch: InsertReceipt(batch.batch_id, len(batch.rows) - 1, batch.digest), "partial ClickHouse insert"),
    ],
)
def test_real_postgres_receipt_fences_invalid_publications(
    control: PostgresControlPlane,
    receipt: Callable[[HighScaleBatch], InsertReceipt],
    error: str,
) -> None:
    service_id, _ = _initialize(control)
    batch = HighScaleBatch(
        f"batch-{uuid4()}",
        service_id,
        "request",
        "generation-1",
        ({"event_id": "event-1"},),
    )
    store = PostgresBatchManifestStore(control)
    publication = ClickHousePublication(store, _ReceiptClient(receipt(batch)))

    with pytest.raises((ValueError, RuntimeError), match=error):
        publication.publish(batch)

    pending = store.get(batch.batch_id)
    assert pending is not None
    assert pending.status is PublicationStatus.PENDING
    assert pending.visible_rows == 0

    client = _ReceiptClient(InsertReceipt(batch.batch_id, len(batch.rows), batch.digest))
    result = ClickHousePublication(store, client).publish(batch)
    assert result.status is PublicationStatus.VISIBLE
    assert result.rows_visible == 1
    assert store.get(batch.batch_id) == replace(pending, status=PublicationStatus.VISIBLE, visible_rows=1)

    cached = _ReceiptClient(InsertReceipt("must-not-be-used", 0, "sha256:unused"))
    repeated = ClickHousePublication(store, cached).publish(batch)
    assert repeated.duplicate
    assert cached.calls == 0


def test_hourly_rollup_fence_exempts_only_the_legacy_nil_batch_marker() -> None:
    fence = visible_rollup_publication_fence_sql("request_aggregate")

    assert fence == (
        f"(batch_id = toUUID('{LEGACY_ROLLUP_BATCH_UUID}') OR "
        "batch_id IN (SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain='request_aggregate' AND publication_state='visible'))"
    )
    assert fence.count(LEGACY_ROLLUP_BATCH_UUID) == 1
    assert fence.count("publication_state='visible'") == 1
