from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from unittest.mock import MagicMock

import pytest

from backend.core.clickhouse_client import ClickHouseError
from backend.core.clickhouse_manifest import Artifact, Dataset, PgManifest
from backend.core.clickhouse_publication import (
    DurableBatch,
    build_batch_id,
    full_rebuild,
    publish_batch,
    readiness,
    replay_unpublished,
    verify_generation,
)
from backend.core.clickhouse_rows import PAYLOAD_COLUMNS, canonical_bytes, canonical_row, digest_rows


def row(ordinal=0):
    return ("raw/fixture.gz", ordinal, "2026-09-01 00:00:00.000000", "US", "192.0.2.1", "/", 1)


def fixture_manifest(service="TestPrototype", rows=None):
    rows = rows or (row(), row(1))
    dataset = Dataset(
        service,
        "dataset",
        "catalog",
        "logs_fixture",
        4,
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime.now(UTC) + timedelta(hours=1),
        len(rows),
        digest_rows(rows),
    )
    content_hash = sha256(b"immutable parquet").hexdigest()
    artifact = Artifact(
        service,
        dataset.dataset_id,
        build_batch_id(service, dataset.dataset_id, content_hash),
        "s3://test-bucket/clickhouse-prototype/fixture.parquet",
        content_hash,
        digest_rows(rows),
        100,
        0,
        len(rows),
    )
    return dataset, artifact, DurableBatch(artifact, rows)


def test_canonical_preserves_null_empty_and_duplicate_ordinals():
    assert digest_rows((row(),)) != digest_rows((row(), row(1)))
    assert canonical_bytes(row()) != canonical_bytes((*row()[:3], None, *row()[4:]))
    assert (
        canonical_row(
            dict(
                zip(
                    ("source_identity", "row_ordinal", "timestamp", "country", "ip", "url", "conn_requests"),
                    row(),
                    strict=True,
                )
            )
        )
        == row()
    )


def test_deterministic_identity_is_tenant_and_dataset_scoped():
    assert build_batch_id("a", "d", "c") == build_batch_id("a", "d", "c")
    assert build_batch_id("a", "d", "c") != build_batch_id("b", "d", "c")
    assert build_batch_id("a", "d", "c") != build_batch_id("a", "e", "c")


def test_production_manifest_rejects_sqlite(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    with pytest.raises(RuntimeError, match="Postgres"):
        PgManifest()


def test_bad_payload_never_claims_or_inserts():
    dataset, artifact, batch = fixture_manifest()
    store = MagicMock()
    store.artifact.return_value = artifact
    client = MagicMock()
    with pytest.raises(ValueError, match="payload"):
        publish_batch(dataset.service_id, replace(batch, rows=(row(),)), generation="g", store=store, client=client)
    store.claim.assert_not_called()
    client.insert_rows.assert_not_called()


def test_conflicting_payload_never_claims_even_with_same_count():
    dataset, artifact, batch = fixture_manifest()
    store = MagicMock()
    store.artifact.return_value = artifact
    client = MagicMock()
    with pytest.raises(ValueError, match="payload"):
        publish_batch(
            dataset.service_id, replace(batch, rows=(row(), row(3))), generation="g", store=store, client=client
        )
    store.claim.assert_not_called()


def test_insert_failure_records_sanitized_failure_and_raises(monkeypatch):
    dataset, artifact, batch = fixture_manifest()
    store = MagicMock()
    store.artifact.return_value = artifact
    store.claim.return_value = 1
    store.generation.return_value = {"target_identity": "target"}
    monkeypatch.setattr("backend.core.clickhouse_publication.target_identity", lambda c: "target")
    client = MagicMock()
    client.insert_rows.side_effect = RuntimeError("credentials must not escape")
    with pytest.raises(RuntimeError, match="publication failed: RuntimeError"):
        publish_batch(dataset.service_id, batch, generation="g", store=store, client=client)
    assert store.finish.call_args.kwargs["error"] == "RuntimeError"
    assert store.finish.call_args.kwargs["digest"] is None


def test_expiry_during_target_verification_is_ineligible(monkeypatch):
    dataset, _, _ = fixture_manifest()
    store = MagicMock()
    store.selected.return_value = {"dataset_id": dataset.dataset_id, "generation": "g"}
    store.dataset.return_value = dataset

    class Clock:
        @staticmethod
        def now(tz):
            return dataset.expires_at + timedelta(seconds=1)

    def slow_verification(*args, **kwargs):
        monkeypatch.setattr("backend.core.clickhouse_publication.datetime", Clock)
        return dataset

    monkeypatch.setattr("backend.core.clickhouse_publication.verify_generation", slow_verification)
    result = readiness(
        dataset.service_id,
        start=dataset.coverage_start,
        end=dataset.coverage_end,
        source_snapshot=dataset.source_snapshot,
        catalog_identity=dataset.catalog_identity,
        source_table=dataset.source_table,
        store=store,
        client=MagicMock(),
    )
    assert not result.eligible
    assert result.reason == "expired_or_unsupported_dataset"


def test_readiness_outage_is_not_ineligible_data(monkeypatch):
    dataset, _, _ = fixture_manifest()
    store = MagicMock()
    store.selected.return_value = {"dataset_id": dataset.dataset_id, "generation": "g"}
    store.dataset.return_value = dataset
    monkeypatch.setattr(
        "backend.core.clickhouse_publication.verify_generation",
        MagicMock(side_effect=ClickHouseError("timeout", query_id="test")),
    )
    with pytest.raises(ClickHouseError):
        readiness(
            dataset.service_id,
            start=dataset.coverage_start,
            end=dataset.coverage_end,
            source_snapshot=dataset.source_snapshot,
            catalog_identity=dataset.catalog_identity,
            source_table=dataset.source_table,
            store=store,
            client=MagicMock(),
        )


@pytest.fixture
def published_fixture(monkeypatch):
    dataset, artifact, batch = fixture_manifest()
    store, client = MagicMock(), MagicMock()
    store.artifact.return_value = artifact
    store.artifacts.return_value = [artifact]
    store.dataset.return_value = dataset
    store.generation.return_value = {"target_identity": "target", "dataset_id": dataset.dataset_id}
    store.claim.return_value = 1
    store.finish.return_value = True
    store.activate.return_value = True
    store.selected.return_value = {"dataset_id": dataset.dataset_id, "generation": "g"}
    monkeypatch.setattr("backend.core.clickhouse_publication.target_identity", lambda c: "target")

    def execute(sql, params):
        assert "log_facts FINAL" in sql
        assert params["service"] == dataset.service_id
        if "SELECT count()" in sql:
            return [{"n": len(batch.rows)}]
        return [dict(zip(PAYLOAD_COLUMNS, row, strict=True)) for row in batch.rows]

    client.execute.side_effect = execute
    return dataset, artifact, batch, store, client


def test_successful_publication_checks_actual_ordered_payload(published_fixture):
    dataset, artifact, batch, store, client = published_fixture
    result = publish_batch(dataset.service_id, batch, generation="g", store=store, client=client)
    assert result.status == "published" and result.row_count == 2 and result.fence == 1
    assert store.finish.call_args.kwargs["digest"] == artifact.canonical_digest
    client.insert_rows.assert_called_once()
    store.claim.return_value = None
    assert publish_batch(dataset.service_id, batch, generation="g", store=store, client=client).status == "not_claimed"
    client.insert_rows.assert_called_once()


@pytest.mark.parametrize("outcome", ["published", "stale", "not_claimed", "failed"])
def test_publication_metric_includes_preclaim_errors(published_fixture, monkeypatch, outcome):
    dataset, _, batch, store, client = published_fixture
    record = MagicMock()
    monkeypatch.setattr("backend.core.clickhouse_publication.record_publication", record)
    if outcome == "not_claimed":
        store.claim.return_value = None
    elif outcome == "stale":
        store.finish.return_value = False
    elif outcome == "failed":
        store.artifact.side_effect = ValueError("manifest unavailable")
    if outcome == "failed":
        with pytest.raises(ValueError, match="manifest unavailable"):
            publish_batch(dataset.service_id, batch, generation="g", store=store, client=client)
        store.claim.assert_not_called()
    else:
        assert publish_batch(dataset.service_id, batch, generation="g", store=store, client=client).status == outcome
    record.assert_called_once_with(outcome)


def test_publication_telemetry_and_failure_recording_never_mask_result(published_fixture, monkeypatch):
    dataset, _, batch, store, client = published_fixture
    monkeypatch.setattr(
        "backend.core.clickhouse_publication.record_publication", MagicMock(side_effect=RuntimeError("telemetry"))
    )
    monkeypatch.setattr("backend.core.clickhouse_publication.logger.info", MagicMock(side_effect=RuntimeError("log")))
    assert publish_batch(dataset.service_id, batch, generation="g", store=store, client=client).status == "published"
    client.insert_rows.side_effect = ValueError("original")
    store.finish.side_effect = RuntimeError("manifest unavailable")
    with pytest.raises(RuntimeError, match="publication failed: ValueError"):
        publish_batch(dataset.service_id, batch, generation="g", store=store, client=client)


def test_generation_verification_rejects_foreign_membership_and_digest(published_fixture):
    dataset, artifact, _, store, client = published_fixture
    assert verify_generation(dataset.service_id, "g", store=store, client=client) == dataset
    store.artifacts.return_value = [replace(artifact, ordinal_start=1)]
    with pytest.raises(ValueError, match="coverage"):
        verify_generation(dataset.service_id, "g", store=store, client=client)
    store.artifacts.return_value = [artifact]
    store.dataset.return_value = replace(dataset, canonical_digest="wrong")
    with pytest.raises(ValueError, match="digest"):
        verify_generation(dataset.service_id, "g", store=store, client=client)


def test_full_rebuild_and_resume_keep_distinct_generation(published_fixture):
    dataset, artifact, batch, store, client = published_fixture
    store.begin_generation.return_value = "new-generation"
    store.pending.side_effect = [[artifact.batch_id], []]
    result = full_rebuild(
        dataset.service_id, dataset.dataset_id, loader=lambda artifact: batch, store=store, client=client
    )
    assert result.generation == "new-generation"
    assert result.activated and result.published == 1
    store.begin_generation.assert_called_once_with(dataset.service_id, dataset.dataset_id, "target")
    store.pending.side_effect = [[artifact.batch_id], ["later"]]
    result = replay_unpublished(
        dataset.service_id, generation="g", loader=lambda artifact: batch, store=store, client=client
    )
    assert not result.activated
    with pytest.raises(ValueError, match="limit"):
        full_rebuild(
            dataset.service_id, dataset.dataset_id, loader=lambda artifact: batch, limit=0, store=store, client=client
        )


def test_readiness_checks_all_eligibility_dimensions(published_fixture):
    dataset, _, _, store, client = published_fixture
    kwargs = dict(
        start=dataset.coverage_start,
        end=dataset.coverage_end,
        source_snapshot=dataset.source_snapshot,
        catalog_identity=dataset.catalog_identity,
        source_table=dataset.source_table,
        store=store,
        client=client,
    )
    assert readiness(dataset.service_id, **kwargs).eligible
    assert readiness(dataset.service_id, **{**kwargs, "source_snapshot": 99}).reason == "source_snapshot_mismatch"
    assert (
        readiness(dataset.service_id, **{**kwargs, "start": dataset.coverage_start - timedelta(days=1)}).reason
        == "outside_coverage"
    )
    client.execute.side_effect = RuntimeError("target unavailable")
    assert readiness(dataset.service_id, **kwargs).reason == "target_unverified"
    store.dataset.side_effect = ValueError("expired")
    assert readiness(dataset.service_id, **kwargs).reason == "expired_or_unsupported_dataset"
    store.selected.return_value = None
    assert readiness(dataset.service_id, **kwargs).reason == "no_active_generation"
