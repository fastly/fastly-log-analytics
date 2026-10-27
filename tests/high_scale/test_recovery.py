"""Regression coverage for the native RUM chart producer, fields, filters, environments, and late discovery."""

from __future__ import annotations

import hashlib
import json
from dataclasses import make_dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest
from backend.core.rum_ingest import extract_metrics_from_faro_payload
from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import (
    ArchivePublication,
    InMemoryObjectStore,
    write_batch_archive_checkpoint,
)
from backend.high_scale.decoder import decode_source_object
from backend.high_scale.publication import (
    _DOMAIN_COLUMNS,
    ClickHousePublication,
    InMemoryBatchManifestStore,
    InsertReceipt,
    PartialInsert,
    _batch_uuid,
    _event_uuid,
    _rows_for_domain,
)
from backend.high_scale.replay import replay_manifest, replay_performance_projections
from backend.high_scale.rum_recovery import RumRecoveryBatchAdapter, _content, execute_rum_recovery, plan_rum_recovery
from structlog.testing import capture_logs

from backend import config
from backend.core import clickhouse_client
from backend.high_scale import postgres_control, registry_bootstrap, rum_recovery
from backend.high_scale import publication as pub_mod
from backend.high_scale.recovery import (
    RecoveryCapacityMeasurement,
    RecoveryReport,
    evaluate_recovery_capacity,
    purge_cmcd_projections,
    purge_origin_projections,
    purge_rum_facts,
    rebuild_cmcd_projections_from_fos,
    rebuild_origin_projections_from_fos,
    rebuild_performance_projections_from_fos,
    rebuild_rum_facts_from_fos,
    rebuild_serving_state_from_fos,
)
from tests.high_scale.conftest import (
    InMemoryManifestCatalog,
    make_test_archive_manifest,
)
from tests.high_scale.conftest import (
    ReplayClickHouseClient as ReplayClient,
)
from tests.high_scale.conftest import (
    event_digest as _event_digest,
)
from tests.high_scale.conftest import (
    make_archive_parquet_bytes as _parquet,
)
from tests.high_scale.conftest import (
    sha256_checksum as _checksum,
)

_TREND_KEYS = ("bucket", "lcp", "cls", "views", "pageviews", "interactions", "errors")


import tempfile
from dataclasses import replace

from backend.high_scale.publication import HighScaleBatch, PublicationStatus

from backend.high_scale.recovery import replay_origin_projections

# ── Merged from tests/high_scale/test_recovery.py ──

NOW = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)
_manifest = partial(
    make_test_archive_manifest, retention_deadline=NOW + timedelta(days=1), deletion_deadline=NOW + timedelta(days=2)
)
_Catalog = partial(InMemoryManifestCatalog, filter_source=False)


def _pub_env(*items, fail_on: set[str] | None = None, client=None):
    archive = ArchivePublication(InMemoryObjectStore())
    manifests = []
    for m, b in items:
        archive.publish(m, b)
        manifests.append(m)
    ch = client or ReplayClient(fail_on=fail_on)
    return _Catalog(tuple(manifests)), archive, ClickHousePublication(InMemoryBatchManifestStore(), ch), ch


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        return []


class _RecordingReplayClient(ReplayClient):
    def __init__(self) -> None:
        super().__init__()
        self.batches: list = []

    def insert(self, batch):
        self.batches.append(batch)
        return super().insert(batch)


def _recent_and_warm():
    return (
        _manifest("recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)),
        _manifest(
            "warm", NOW - timedelta(days=10), NOW - timedelta(days=10) + timedelta(minutes=1), ({"event_id": "2"},)
        ),
    )


def test_recovery_prioritizes_recent_history_and_reports_full_completion() -> None:
    catalog, archive, publication, _ = _pub_env(*_recent_and_warm())
    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert report.manifests_replayed == 2 and report.rows_replayed == 2
    assert report.triage_available and report.raw_query_available and report.warm_history_available
    assert report.full_rebuild_complete and report.errors == ()


def test_recovery_reports_partial_availability_when_warm_tier_fails() -> None:
    catalog, archive, publication, _ = _pub_env(*_recent_and_warm(), fail_on={"warm"})
    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert report.triage_available and report.raw_query_available
    assert not report.warm_history_available and not report.full_rebuild_complete
    assert len(report.errors) == 1 and "warm" in report.errors[0]


def test_recovery_reports_no_triage_when_recent_tier_fails() -> None:
    catalog, archive, publication, _ = _pub_env(_recent_and_warm()[0], fail_on={"recent"})
    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert not report.triage_available and not report.raw_query_available and not report.full_rebuild_complete


def test_recovery_reinserts_rows_whose_manifest_was_already_replayed_once() -> None:
    rec = _recent_and_warm()[0]
    catalog, archive, publication, client = _pub_env(rec)
    first = replay_manifest(rec[0], archive, publication)
    assert first.publication.duplicate is False and client.insert_calls == ["replay:recent"]

    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert report.rows_replayed == 1 and report.manifests_replayed == 1 and report.full_rebuild_complete
    assert len(client.insert_calls) == 2 and client.insert_calls[0] != client.insert_calls[1]


def test_resuming_a_rebuild_with_the_same_rebuild_id_stays_idempotent() -> None:
    catalog, archive, publication, client = _pub_env(_recent_and_warm()[0])
    first = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert len(client.insert_calls) == 1
    second = rebuild_serving_state_from_fos(
        catalog, archive, publication, service_id="svc", domain="request", now=NOW, rebuild_id=first.rebuild_id
    )
    assert len(client.insert_calls) == 1 and second.manifests_replayed == 1 and second.rebuild_id == first.rebuild_id


def test_recovery_with_no_archived_history_is_a_trivially_complete_rebuild() -> None:
    catalog, archive, publication, _ = _pub_env()
    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert report.manifests_replayed == 0 and report.full_rebuild_complete and report.ingest_accepted


def test_origin_projection_rebuild_is_resumable_without_replaying_request_facts() -> None:
    rec = _manifest(
        "recent-origin",
        NOW - timedelta(hours=1),
        NOW - timedelta(minutes=59),
        ({"timestamp": (NOW - timedelta(hours=1)).isoformat(), "url": "/slow", "ottfb": 2500, "ost": 503},),
    )
    catalog, archive, publication, client = _pub_env(rec)
    first = rebuild_origin_projections_from_fos(catalog, archive, publication, service_id="svc", now=NOW)
    second = rebuild_origin_projections_from_fos(
        catalog, archive, publication, service_id="svc", now=NOW, rebuild_id=first.rebuild_id
    )
    assert first.manifests_replayed == second.manifests_replayed == 1
    assert first.rows_replayed == second.rows_replayed == 3
    assert client.insert_calls == [
        f"rebuild-origin:{first.rebuild_id}:recent-origin:summary",
        f"rebuild-origin:{first.rebuild_id}:recent-origin:dimensions",
    ]


def _performance_events() -> tuple[dict, ...]:
    return (
        dict(
            event_id="perf-1",
            timestamp=(NOW - timedelta(hours=1)).isoformat(),
            url="/slow",
            cache="MISS",
            elapsed=250_000,
            ttfb=0.1,
            ottfb=40_000,
            ottlb=90_000,
        ),
    )


def test_performance_projection_rebuild_publishes_only_the_new_tables_and_is_resumable() -> None:
    rec = _manifest("recent-perf", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), _performance_events())
    catalog, archive, publication, client = _pub_env(rec)
    first = rebuild_performance_projections_from_fos(catalog, archive, publication, service_id="svc", now=NOW)
    second = rebuild_performance_projections_from_fos(
        catalog, archive, publication, service_id="svc", now=NOW, rebuild_id=first.rebuild_id
    )
    assert first.manifests_replayed == second.manifests_replayed == 1
    assert first.rows_replayed == second.rows_replayed == 2 and first.full_rebuild_complete
    assert client.insert_calls == [
        f"rebuild-performance:{first.rebuild_id}:recent-perf:summary",
        f"rebuild-performance:{first.rebuild_id}:recent-perf:scatter",
    ]


def test_performance_projection_rebuild_reports_a_failed_manifest_without_stopping() -> None:
    bad = _manifest("bad-perf", NOW - timedelta(hours=2), NOW - timedelta(hours=1), _performance_events())
    good = _manifest("good-perf", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), _performance_events())
    catalog, archive, publication, _ = _pub_env(bad, good, fail_on={"bad-perf"})
    report = rebuild_performance_projections_from_fos(catalog, archive, publication, service_id="svc", now=NOW)
    assert report.manifests_replayed == 1 and not report.full_rebuild_complete
    assert len(report.errors) == 1 and "bad-perf" in report.errors[0]


def test_performance_projection_replay_rejects_non_request_manifests() -> None:
    rum = SimpleNamespace(source=SimpleNamespace(domain="rum_vitals"))
    _, archive, publication, _ = _pub_env()
    with pytest.raises(ValueError, match="request manifests"):
        replay_performance_projections(rum, archive, publication, rebuild_id="r")


def test_purge_origin_projections_deletes_only_the_service_window_from_origin_tables() -> None:
    client = _Recorder()
    purge_origin_projections(client, service_id="svc", since=NOW - timedelta(days=30))
    assert [sql.split()[2].strip("`") for sql, _ in client.calls] == [
        "origin_minute_summary",
        "origin_minute_dimensions",
    ]
    for sql, params in client.calls:
        assert sql.startswith("ALTER TABLE") and "mutations_sync=2" in sql
        assert "DELETE WHERE service_id={service_id:String} AND bucket_start >= {since:DateTime64(3)}" in sql
        assert params == {"service_id": "svc", "since": NOW - timedelta(days=30)}


def _cmcd_events() -> tuple[dict, ...]:
    ts = (NOW - timedelta(hours=1)).isoformat()
    return (
        {
            "event_id": "evt-1",
            "service_id": "svc",
            "timestamp": ts,
            "country": "US",
            "asn": 7922,
            "cmcd": {"sid": "s-1", "br": "1200", "bs": None},
        },
        {
            "event_id": "evt-2",
            "service_id": "svc",
            "timestamp": ts,
            "country": "DE",
            "asn": None,
            "cmcd": {"sid": None, "br": None, "bs": None},
        },
        {"event_id": "evt-3", "service_id": "svc", "timestamp": ts, "country": "FR", "asn": 3215, "cmcd": None},
    )


def test_cmcd_projection_rebuild_publishes_geography_enriched_rows_and_is_resumable() -> None:
    rec = _manifest("recent-cmcd", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), _cmcd_events())
    catalog, archive, publication, client = _pub_env(rec, client=_RecordingReplayClient())
    first = rebuild_cmcd_projections_from_fos(catalog, archive, publication, service_id="svc", now=NOW)
    second = rebuild_cmcd_projections_from_fos(
        catalog, archive, publication, service_id="svc", now=NOW, rebuild_id=first.rebuild_id
    )
    assert first.full_rebuild_complete and first.manifests_replayed == second.manifests_replayed == 1
    assert first.rows_replayed == second.rows_replayed == 1
    assert client.insert_calls == [f"rebuild-cmcd:{first.rebuild_id}:recent-cmcd"]
    (batch,) = client.batches
    assert batch.domain == "cmcd"
    (row,) = batch.rows
    assert row["request_event_id"] == "evt-1" and row["country"] == "US" and row["asn"] == 7922
    assert row["fields"] == {"sid": "s-1", "br": "1200"}


def test_cmcd_projection_rebuild_reports_a_failing_manifest_without_stopping() -> None:
    rec = _manifest("recent-cmcd", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), _cmcd_events())
    catalog, archive, publication, _ = _pub_env(rec, fail_on={"rebuild-cmcd"})
    report = rebuild_cmcd_projections_from_fos(catalog, archive, publication, service_id="svc", now=NOW)
    assert not report.full_rebuild_complete and report.manifests_replayed == 0 and "recent-cmcd" in report.errors[0]


def test_purge_cmcd_projections_deletes_only_the_service_window() -> None:
    client = _Recorder()
    purge_cmcd_projections(client, service_id="svc", since=NOW - timedelta(days=30))
    ((sql, params),) = client.calls
    assert sql.startswith("ALTER TABLE `cmcd_projection_facts` DELETE WHERE service_id={service_id:String}")
    assert "event_timestamp >= {since:DateTime64(3)}" in sql and "mutations_sync=2" in sql
    assert params == {"service_id": "svc", "since": NOW - timedelta(days=30)}


@pytest.mark.parametrize(("domain", "table"), [("rum_vitals", "rum_vitals_facts"), ("rum_errors", "rum_error_facts")])
def test_purge_rum_facts_deletes_only_the_service_window_of_one_rum_table(domain: str, table: str) -> None:
    client = _Recorder()
    purge_rum_facts(client, domain=domain, service_id="svc", since=NOW - timedelta(days=30))
    ((sql, params),) = client.calls
    assert sql.startswith(f"ALTER TABLE `{table}` DELETE WHERE service_id={{service_id:String}}")
    assert "event_timestamp >= {since:DateTime64(3)}" in sql and "mutations_sync=2" in sql
    assert params == {"service_id": "svc", "since": NOW - timedelta(days=30)}


def test_purge_rum_facts_rejects_other_domains() -> None:
    with pytest.raises(ValueError, match="rum_vitals or rum_errors"):
        purge_rum_facts(object(), domain="request", service_id="svc", since=NOW)  # type: ignore[arg-type]


def test_rum_fact_rebuild_replays_the_rum_domain_over_the_requested_window() -> None:
    rec = _manifest("rum-1", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},))
    warm = _manifest(
        "rum-2", NOW - timedelta(days=10), NOW - timedelta(days=10) + timedelta(minutes=1), ({"event_id": "2"},)
    )
    _, archive, publication, _ = _pub_env(rec, warm)
    seen: list[str] = []

    class _TrackingCatalog:
        def manifests_covering(self, service_id, domain, start, end):
            seen.append(domain)
            return tuple(m for m in (rec[0], warm[0]) if m.coverage_end > start and m.coverage_start < end)

    report = rebuild_rum_facts_from_fos(
        _TrackingCatalog(),
        archive,
        publication,
        service_id="svc",
        domain="rum_vitals",
        now=NOW,
        window=timedelta(days=30),
    )
    assert set(seen) == {"rum_vitals"} and report.full_rebuild_complete and report.manifests_replayed == 2
    with pytest.raises(ValueError, match="rum_vitals or rum_errors"):
        rebuild_rum_facts_from_fos(
            _TrackingCatalog(), archive, publication, service_id="svc", domain="request", now=NOW
        )
    assert rebuild_rum_facts_from_fos(
        _TrackingCatalog(),
        archive,
        publication,
        service_id="svc",
        domain="rum_errors",
        now=NOW,
        window=timedelta(hours=6),
    ).full_rebuild_complete


def test_performance_projection_rebuild_backfills_asn_rows_but_never_republishes_url_rows() -> None:
    events = tuple({**event, "asn": 7922, "url": "/slow"} for event in _performance_events())
    rec = _manifest("asn-perf", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), events)
    catalog, archive, publication, client = _pub_env(rec)
    report = rebuild_performance_projections_from_fos(
        catalog, archive, publication, service_id="svc", now=NOW, rebuild_id="r1"
    )
    assert report.full_rebuild_complete
    assert client.insert_calls == [
        "rebuild-performance:r1:asn-perf:summary",
        "rebuild-performance:r1:asn-perf:scatter",
        "rebuild-performance:r1:asn-perf:asn",
    ]


def _make_archive_manifest(mid: str, domain: str, key: str, rows: tuple[dict, ...]):
    artifact_bytes = _parquet(rows)
    source = ArchiveSourceObject("svc", domain, key, "sha256:source", 4, "v1")
    manifest = ArchiveManifest(
        mid,
        source,
        ArchiveArtifact(
            f"s3://archive/artifacts/{mid}.parquet",
            _checksum(artifact_bytes),
            len(artifact_bytes),
            len(rows),
            len(rows),
            _event_digest(rows),
            f"{domain}.v1",
            "normalize.v1",
        ),
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime(2026, 9, 3, tzinfo=UTC),
        4,
    )
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(manifest, artifact_bytes)
    return manifest, archive


def test_replay_rebuilds_visibility_from_archive_only() -> None:
    rows = ({"event_id": "event-1", "status": 200}, {"event_id": "event-2", "status": 500})
    manifest, archive = _make_archive_manifest("manifest-1", "request", "raw/request/a.gz", rows)
    result = replay_manifest(manifest, archive, ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient()))
    assert result.rows_read == 2 and result.publication.rows_visible == 2


def test_replay_with_a_custom_batch_id_reinserts_after_a_prior_replay_is_marked_visible() -> None:
    manifest, archive = _make_archive_manifest(
        "manifest-1", "request", "raw/request/a.gz", ({"event_id": "event-1", "status": 200},)
    )
    inserted: list[int] = []

    class CountingClient:
        def insert(self, batch):
            inserted.append(len(batch.rows))
            return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    publication = ClickHousePublication(InMemoryBatchManifestStore(), CountingClient())
    assert not replay_manifest(manifest, archive, publication).publication.duplicate and inserted == [1]
    assert replay_manifest(manifest, archive, publication).publication.duplicate and inserted == [1]
    assert not replay_manifest(
        manifest, archive, publication, batch_id=f"rebuild:xyz:{manifest.manifest_id}"
    ).publication.duplicate
    assert inserted == [1, 1]


def test_replay_manifest_filters_mixed_rum_rows_by_serving_domain() -> None:
    vitals_only = ({"event_id": "vital-1", "metric_name": "LCP", "metric_value": 1200.0, "error_message": ""},)
    manifest, archive = _make_archive_manifest("manifest-rum-err-empty", "rum_errors", "raw/rum/a.gz", vitals_only)
    inserted: list[int] = []

    class StrictRumErrorClient:
        def insert(self, batch):
            if any(not row.get("error_message") for row in batch.rows):
                raise ValueError("rum_errors batch contained non-error row")
            inserted.append(len(batch.rows))
            return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    result = replay_manifest(
        manifest, archive, ClickHousePublication(InMemoryBatchManifestStore(), StrictRumErrorClient())
    )
    assert result.rows_read == 0 and result.publication.rows_visible == 0 and inserted == []


def _rum_repair_archive(tmp_path, *, faro_body=None):
    b1 = faro_body or {"measurements": [{"type": "web-vitals", "values": {"lcp": 12, "cls": 0.1}}]}
    source_payload = (
        f"\n{json.dumps({'timestamp': '2026-09-14T11:00:00Z', 'rum_body': json.dumps(b1)})}\n"
        f"{json.dumps({'timestamp': '2026-09-14T11:00:01Z', 'rum_body': json.dumps({'exceptions': [{'value': 'boom'}]})})}"
    ).encode()
    source = ArchiveSourceObject(
        "svc", "rum_errors", "raw/rum/repair.gz", hashlib.sha256(source_payload).hexdigest(), len(source_payload), "v1"
    )
    decoded = decode_source_object(source, source_payload, transform_version="normalize.v1")
    rows = [dict(event, _record_kind="event") for event in decoded.events]
    manifest = write_batch_archive_checkpoint(
        tmp_path,
        service_id="svc",
        domain="rum_errors",
        source_batches=[(source, rows)],
        archive_epoch=9,
        schema_version="rum.v1",
        transform_version="normalize.v1",
        coverage_start=NOW,
        coverage_end=NOW,
    )
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(manifest, Path(manifest.artifact.uri.removeprefix("file://")).read_bytes())
    return manifest, archive


def test_scoped_rum_repair_uses_actual_standard_faro_event_projections(tmp_path) -> None:
    body = {
        "measurements": [{"type": "web-vitals", "values": {"lcp": 12, "cls": 0.1, "delta": 9}}],
        "events": [
            {"name": "click"},
            {"name": "faro.performance.navigation", "attributes": {"domContentLoaded": 40, "loadEvent": 50}},
        ],
    }
    manifest, archive = _rum_repair_archive(tmp_path, faro_body=body)
    plan = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    vitals = [row for batch in plan.batches if batch.domain == "rum_vitals" for row in batch.rows]
    assert [(r["metric_name"], r["metric_value"]) for r in vitals] == [
        (m["metric_name"], m["metric_value"]) for m in extract_metrics_from_faro_payload(body, {})
    ]
    assert plan.candidate_counts == {"rum_vitals": 5, "rum_errors": 1}
    assert next(r for r in vitals if r["metric_name"] == "event_click")["metric_value"] == 1.0
    assert all(r["metric_name"] not in {"exception", "delta"} for r in vitals)


def test_scoped_rum_repair_checks_other_batches_and_is_idempotent(tmp_path) -> None:
    manifest, archive = _rum_repair_archive(tmp_path)
    empty = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    assert empty.candidate_counts == {"rum_vitals": 2, "rum_errors": 1}
    assert all(batch.generation == "2" for batch in empty.batches)
    assert {row["line_ordinal"] for batch in empty.batches for row in batch.rows} == {1, 2}

    persisted = [
        dict(row, repair_visible=True, batch_id="00000000-0000-0000-0000-000000000099") for row in empty.expected_facts
    ]
    errors_only = [row for row in persisted if "error_message" in row]
    plan = plan_rum_recovery(
        manifest, archive, owner_epoch=2, read_facts=lambda d, _: errors_only if d == "rum_errors" else (), now=NOW
    )
    assert plan.missing_counts == {"rum_vitals": 2, "rum_errors": 0}
    assert plan.exact_identity_counts == {"rum_vitals": 0, "rum_errors": 1} and len(plan.batches) == 1
    assert plan.plan_digest == empty.plan_digest and plan.batches[0].batch_id == empty.batches[0].batch_id
    complete = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: persisted, now=NOW)
    assert complete.batches == () and complete.missing_counts == {"rum_vitals": 0, "rum_errors": 0}

    for index, fact in enumerate(persisted, start=1):
        fact.update(event_id=f"00000000-0000-0000-0000-{index:012x}", transform_version="older-transform")
    equivalent = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: persisted, now=NOW)
    assert equivalent.batches == () and equivalent.equivalent_identity_counts == {"rum_vitals": 2, "rum_errors": 1}


@pytest.mark.parametrize("visible,copies", [(False, 1), (True, 2)])
def test_scoped_rum_repair_refuses_hidden_or_duplicate_existing_identity(tmp_path, visible, copies) -> None:
    manifest, archive = _rum_repair_archive(tmp_path)
    plan = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    fact = dict(plan.expected_facts[0], repair_visible=visible)
    with pytest.raises(ValueError, match="ambiguous or unpublished"):
        plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: [fact] * copies, now=NOW)


def test_scoped_rum_repair_refuses_unretained_event_times(tmp_path) -> None:
    manifest, archive = _rum_repair_archive(tmp_path)
    with pytest.raises(ValueError, match="outside retained"):
        plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW + timedelta(days=31))


@pytest.mark.parametrize("domain", ["rum_vitals", "rum_errors"])
def test_scoped_rum_content_normalizes_float64_json_integer_representation(domain) -> None:
    expected = {"event_timestamp": NOW, "ttfb": 25.0, "metric_value": 12.0}
    physical = {"event_timestamp": NOW, "ttfb": 25, "metric_value": 12}
    assert _content(expected, domain) == _content(physical, domain)
    assert _content(expected, domain) != _content(dict(physical, ttfb=26), domain)


def test_scoped_rum_apply_requires_reviewed_digest_and_verifies_retry(tmp_path) -> None:
    manifest, archive = _rum_repair_archive(tmp_path)
    persisted: list[dict] = []
    make_plan = partial(plan_rum_recovery, manifest, archive, owner_epoch=2, read_facts=lambda *_: persisted, now=NOW)
    initial = make_plan()
    publication = MagicMock()
    assert execute_rum_recovery(make_plan, publication, apply=False)["applied"] is False
    publication.publish.assert_not_called()
    with pytest.raises(ValueError, match="reviewed plan"):
        execute_rum_recovery(make_plan, publication, apply=True, approve_digest="wrong")
    publication.publish.assert_not_called()

    def publish(batch):
        ids = {_event_uuid(row["event_id"], field="event_id") for row in batch.rows}
        persisted.extend(dict(row, repair_visible=True) for row in initial.expected_facts if row["event_id"] in ids)

    publication.publish.side_effect = publish
    result = execute_rum_recovery(make_plan, publication, apply=True, approve_digest=initial.plan_digest)
    assert result["remaining_counts"] == {"rum_vitals": 0, "rum_errors": 0} and publication.publish.call_count == 2
    execute_rum_recovery(make_plan, publication, apply=True, approve_digest=initial.plan_digest)
    assert publication.publish.call_count == 2


def test_scoped_rum_cli_defaults_to_dryrun_and_redacts_dependency_errors(monkeypatch, capsys) -> None:
    from scripts import recover_high_scale_rum as cli

    run = MagicMock(return_value={"applied": False, "candidate_counts": {"rum_vitals": 2, "rum_errors": 1}})
    monkeypatch.setattr(cli, "recover", run)
    assert cli.main(["--manifest-prefix", "a" * 8]) == 0
    run.assert_called_once_with("a" * 8, apply=False, approve_digest=None)
    assert json.loads(capsys.readouterr().out)["applied"] is False
    run.side_effect = ValueError("private dependency response must not escape")
    assert cli.main(["--manifest-prefix", "a" * 8]) == 1
    assert json.loads(capsys.readouterr().out) == {"status": "blocked", "error_type": "ValueError"}


def test_scoped_rum_cli_uses_canonical_source_owner_epoch_column(tmp_path, monkeypatch) -> None:
    from scripts import recover_high_scale_rum as cli

    manifest, archive = _rum_repair_archive(tmp_path)
    connection = MagicMock()
    connection.__enter__.return_value = connection

    def execute(query, params=None):
        cursor = MagicMock()
        if "FROM high_scale_archive_manifests" in query:
            cursor.fetchall.return_value = [{}]
        elif "FROM high_scale_ownership" in query:
            cursor.fetchone.return_value = {"owner_epoch": 2, "current_owner": "high_scale"}
        elif "FROM high_scale_source_objects" in query:
            assert "archive_owner_epoch" not in query and "archive_manifest_id,owner_epoch" in query
            cursor.fetchall.return_value = [
                {
                    "object_key": s.key,
                    "status": "source_deleted",
                    "archive_manifest_id": manifest.manifest_id,
                    "owner_epoch": 2,
                }
                for s in manifest.sources
            ]
        return cursor

    connection.execute.side_effect = execute
    monkeypatch.setenv("METADATA_DSN", "postgresql://unused")
    monkeypatch.setattr(psycopg, "connect", MagicMock(return_value=connection))
    monkeypatch.setattr(
        postgres_control,
        "_archive_from_row",
        lambda _: SimpleNamespace(manifest=manifest, state="source_deleted", owner_epoch=2),
    )
    monkeypatch.setattr(postgres_control, "PostgresControlPlane", MagicMock())
    monkeypatch.setattr(registry_bootstrap, "_default_archive_factory", lambda _: archive)
    monkeypatch.setattr(
        config, "load_clickhouse_config", lambda: make_dataclass("RecoveryTestSettings", [("query_timeout_s", int)])(5)
    )
    client = MagicMock()
    initial_plan = rum_recovery.plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    error_fact = next(fact for fact in initial_plan.expected_facts if fact["_repair_domain"] == "rum_errors")

    def read_clickhouse(query, params=None, *, settings=None):
        if "high_scale_batch_publications" in query:
            assert "batch_id={batch_id:String}" in query and "batch_id:UUID" not in query
            return [{"publication_state": "visible", "expected_rows": 1, "visible_rows": 1}]
        return [dict(error_fact)] if "rum_error_facts" in query else []

    client.execute.side_effect = read_clickhouse
    monkeypatch.setattr(clickhouse_client, "ClickHouseClient", MagicMock(return_value=client))
    monkeypatch.setattr(pub_mod, "PostgresBatchManifestStore", MagicMock())
    serving = MagicMock()
    monkeypatch.setattr(pub_mod, "ClickHousePublication", MagicMock(return_value=serving))
    original_plan = rum_recovery.plan_rum_recovery
    monkeypatch.setattr(rum_recovery, "plan_rum_recovery", lambda *a, **kw: original_plan(*a, **kw, now=NOW))

    report = cli.recover("a" * 8, apply=False, approve_digest=None)
    assert report["read_only"] is True
    assert report["candidate_counts"] == {"rum_vitals": 2, "rum_errors": 1}
    assert report["missing_counts"] == {"rum_vitals": 2, "rum_errors": 0}
    serving.publish.assert_not_called()


def test_scoped_rum_apply_cannot_report_success_on_missing_insert(tmp_path) -> None:
    manifest, archive = _rum_repair_archive(tmp_path)
    make_plan = partial(plan_rum_recovery, manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    with pytest.raises(ValueError, match="post-apply identity verification"):
        execute_rum_recovery(make_plan, MagicMock(), apply=True, approve_digest=make_plan().plan_digest)


class _PartitionedRumClient:
    def __init__(self, rows=()):
        self.rows = [dict(row) for row in rows]
        self.partitions = {"20260901": 71_167, "20260905": 60_000, "20260914": 70_000}
        self.queries, self.inserts = [], []
        self.receipt = None
        self.truncate_insert = self.change_inventory = False
        self.inventory_reads = 0

    def execute(self, query, params=None, *, settings=None):
        self.queries.append((query, params, settings))
        if "system.parts" in query:
            self.inventory_reads += 1
            rows = [{"partition_id": k, "partition_rows": c} for k, c in self.partitions.items()]
            if self.change_inventory and self.inventory_reads % 2 == 0:
                rows.append({"partition_id": "20260930", "partition_rows": 1})
            return rows
        if "high_scale_batch_publications" in query:
            return [] if self.receipt is None else [dict(self.receipt)]
        assert "_partition_id IN" in query and "event_timestamp >" not in query and "source_object_key IN" not in query
        partitions = {v for k, v in params.items() if k.startswith("part")}
        return [
            dict(r)
            for r in self.rows
            if r["event_timestamp"].strftime("%Y%m%d") in partitions
            and str(r["batch_id"]) == params["batch_id"]
            and r["service_id"] == params["service_id"]
        ]

    def insert_rows(self, table, columns, rows):
        self.inserts.append((table, len(rows)))
        if table == "high_scale_batch_publications":
            self.receipt = dict(zip(columns, rows[0], strict=True))
        else:
            self.rows.extend(dict(zip(columns, r, strict=True)) for r in (rows[:1] if self.truncate_insert else rows))

    def delete_batch_rows(self, *_):
        raise AssertionError("repair must never silently delete a partial/conflicting batch")


def _partitioned_rum_adapter(tmp_path, *, physical=()):
    manifest, archive = _rum_repair_archive(tmp_path)
    plan = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    batch = next(b for b in plan.batches if b.domain == "rum_vitals")
    rows = [
        dict(zip(_DOMAIN_COLUMNS[batch.domain], r, strict=True)) for r in _rows_for_domain(batch, _batch_uuid(batch))
    ]
    client = _PartitionedRumClient(physical)
    return RumRecoveryBatchAdapter(client), client, batch, rows


def test_partitioned_rum_count_preserves_global_empty_insert_and_retry(tmp_path) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    assert adapter.inspect_batch(batch) == (0, None)
    assert adapter.insert(batch).rows_inserted == 2 and adapter.insert(batch).rows_inserted == 2
    assert len(client.rows) == 2 and client.inserts.count(("rum_vitals_facts", 2)) == 1
    selected_partitions = set()
    for query, params, settings in client.queries:
        if "system.parts" not in query and "high_scale_batch_publications" not in query:
            selected_partitions.update(v for k, v in params.items() if k.startswith("part"))
        if settings is not None:
            assert settings["max_rows_to_read"] == "100000" and settings["read_overflow_mode"] == "throw"
    assert selected_partitions == set(client.partitions)


@pytest.mark.parametrize("kind", "partial duplicate conflict outside_time outside_source unknown_id".split())
def test_partitioned_rum_count_refuses_bad_existing_rows_without_writes(tmp_path, kind) -> None:
    adapter, client, batch, rows = _partitioned_rum_adapter(tmp_path)
    if kind == "partial":
        rows = rows[:1]
    elif kind == "duplicate":
        rows = [rows[0], rows[0]]
    elif kind == "conflict":
        rows[0]["metric_value"] = 999
    elif kind == "outside_time":
        rows[0]["event_timestamp"] = NOW - timedelta(days=13)
    elif kind == "outside_source":
        rows[0]["source_object_key"] = "raw/rum/unexpected.gz"
    else:
        rows[0].update(event_id="00000000-0000-0000-0000-000000000099", event_timestamp=NOW - timedelta(days=13))
    client.rows = rows
    with pytest.raises((ValueError, PartialInsert)):
        adapter.insert(batch)
    assert client.inserts == []


def test_partitioned_rum_count_does_not_delete_or_publish_partial_insert(tmp_path) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    client.truncate_insert = True
    with pytest.raises(PartialInsert):
        adapter.insert(batch)
    assert client.receipt["publication_state"] == "pending" and len(client.rows) == 1
    before = list(client.inserts)
    with pytest.raises(PartialInsert):
        adapter.insert(batch)
    assert client.inserts == before


def test_partitioned_rum_count_checks_physical_rows_even_with_visible_receipt(tmp_path) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    adapter.insert(batch)
    client.rows.append(dict(client.rows[0]))
    before = list(client.inserts)
    with pytest.raises(ValueError, match="duplicate"):
        adapter.insert(batch)
    assert client.inserts == before


def test_partitioned_rum_count_recovers_full_valid_lost_ack_without_reinserting(tmp_path) -> None:
    adapter, client, batch, rows = _partitioned_rum_adapter(tmp_path)
    client.rows = rows
    assert adapter.inspect_batch(batch) == (2, None) and adapter.insert(batch).rows_inserted == 2
    assert not any(table == "rum_vitals_facts" for table, _ in client.inserts)
    assert client.receipt["publication_state"] == "visible"


def test_partitioned_rum_count_never_swallows_failed_partition_read(tmp_path) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    original = client.execute

    def _execute_failing_bounded_scan(q, *a, **kw):
        if "_partition_id IN" in q:
            raise RuntimeError("bounded scan failed")
        return original(q, *a, **kw)

    client.execute = _execute_failing_bounded_scan
    with pytest.raises(RuntimeError, match="bounded scan failed"):
        adapter.inspect_batch(batch)
    assert client.inserts == []


@pytest.mark.parametrize("unsafe", "changing_inventory oversized_partition too_many_groups too_many_partitions".split())
def test_partitioned_rum_count_refuses_unprovable_inventory(tmp_path, unsafe) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    if unsafe == "changing_inventory":
        client.change_inventory = True
    elif unsafe == "oversized_partition":
        client.partitions = {"20260914": 100_001}
    elif unsafe == "too_many_groups":
        client.partitions = {f"partition-{i}": 90_000 for i in range(7)}
    else:
        client.partitions = {f"partition-{i}": 1 for i in range(33)}
    with pytest.raises(ValueError):
        adapter.inspect_batch(batch)
    assert client.inserts == []


_GOOD = dict(
    events_per_second=2_000_000,
    recovery_window_seconds=900,
    archive_read_events_per_second=300_000_000,
    decode_events_per_second=300_000_000,
    aggregate_events_per_second=300_000_000,
    clickhouse_insert_events_per_second=750_000_000,
    replication_factor=3,
    live_ingest_reservation=0.2,
)


def _measurement(**overrides) -> RecoveryCapacityMeasurement:
    return RecoveryCapacityMeasurement(**{**_GOOD, **overrides})


def _recovery_env(*, fail_on: set[str] | None = None):
    return _pub_env(*_recent_and_warm(), fail_on=fail_on)


def _rebuild(env, **kwargs):
    catalog, archive, publication, _ = env
    return rebuild_serving_state_from_fos(
        catalog, archive, publication, service_id="svc", domain="request", now=NOW, **kwargs
    )


def test_feasible_measurement_with_a_complete_rebuild_makes_the_claim_valid() -> None:
    with capture_logs() as logs:
        report = _rebuild(_recovery_env(), capacity=_measurement())
    assert report.full_rebuild_complete and report.capacity is not None and report.capacity.feasible
    assert report.recovery_claim_valid is True
    gate = next(e for e in logs if e["event"] == "high_scale.recovery_capacity_gate")
    assert gate["feasible"] is True and gate["log_level"] == "info"


def test_infeasible_measurement_flags_the_claim_but_never_blocks_recovery() -> None:
    env = _recovery_env()
    with capture_logs() as logs:
        report = _rebuild(env, capacity=_measurement(clickhouse_insert_events_per_second=400_000_000))
    assert report.full_rebuild_complete and report.rows_replayed == 2 and len(env[3].insert_calls) == 2
    assert report.capacity is not None and report.capacity.limiting_stage == "clickhouse_insert"
    assert report.recovery_claim_valid is False
    gate = next(e for e in logs if e["event"] == "high_scale.recovery_capacity_gate")
    assert gate["log_level"] == "warning" and gate["limiting_stage"] == "clickhouse_insert"


def test_feasible_capacity_does_not_validate_an_incomplete_rebuild() -> None:
    report = _rebuild(_recovery_env(fail_on={"warm"}), capacity=_measurement())
    assert report.capacity is not None and report.capacity.feasible and not report.full_rebuild_complete
    assert report.recovery_claim_valid is False


def test_without_a_measurement_the_report_carries_no_claim() -> None:
    with capture_logs() as logs:
        report = _rebuild(_recovery_env())
    assert report.capacity is None and report.recovery_claim_valid is None
    assert not [e for e in logs if e["event"] == "high_scale.recovery_capacity_gate"]


def test_invalid_measurement_fails_before_any_replay_work() -> None:
    env = _recovery_env()
    with pytest.raises(ValueError, match="event rate and recovery window must be positive"):
        _rebuild(env, capacity=_measurement(events_per_second=0))
    assert env[3].insert_calls == []


def test_origin_projection_rebuild_evaluates_the_same_gate() -> None:
    catalog, archive, publication, _ = _recovery_env()
    report = rebuild_origin_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id="svc",
        now=NOW,
        window=timedelta(hours=2),
        capacity=_measurement(decode_events_per_second=1_000),
    )
    assert (
        report.capacity is not None and report.capacity.limiting_stage == "decode" and not report.recovery_claim_valid
    )


@pytest.fixture
def cli(monkeypatch: pytest.MonkeyPatch):
    from scripts import backfill_high_scale as script

    service = SimpleNamespace(manifest_catalog=object(), archive=object(), client=MagicMock())
    registry = MagicMock()
    registry.resolve.return_value = service
    monkeypatch.setattr(script.app_config, "load_clickhouse_config", lambda: object())
    monkeypatch.setattr(script, "ClickHouseClient", lambda settings: MagicMock())
    monkeypatch.setattr(script, "get_high_scale_service_registry", lambda: registry)
    monkeypatch.setattr(script, "register_high_scale_services_from_environment", lambda *a, **k: None)
    monkeypatch.setattr(script, "PostgresControlPlane", MagicMock)
    monkeypatch.setattr(script, "PostgresBatchManifestStore", lambda control: MagicMock())
    monkeypatch.setattr(script, "AggregateBatchAdapter", lambda client: MagicMock())
    monkeypatch.setattr(script, "ClickHousePublication", lambda manifests, adapter: MagicMock())
    return script


def _run_cli(cli, monkeypatch, tmp_path, measurement: dict | None, *, complete: bool = True):
    seen: dict = {}

    def fake_rebuild(*args, **kwargs):
        seen.update(kwargs)
        cap = kwargs.get("capacity")
        rep = evaluate_recovery_capacity(cap) if cap else None
        return RecoveryReport(
            True,
            complete,
            True,
            complete,
            complete,
            1,
            1,
            (),
            "rb-1",
            capacity=rep,
            recovery_claim_valid=None if rep is None else rep.feasible and complete,
        )

    monkeypatch.setattr(cli, "rebuild_origin_projections_from_fos", fake_rebuild)
    argv = ["backfill", "--service-id", "svc"]
    if measurement is not None:
        path = tmp_path / "capacity.json"
        path.write_text(json.dumps(measurement))
        argv += ["--capacity-json", str(path)]
    monkeypatch.setattr("sys.argv", argv)
    return cli.main(), seen


def test_cli_capacity_and_exit_codes(cli, monkeypatch, tmp_path) -> None:
    code, seen = _run_cli(cli, monkeypatch, tmp_path, _GOOD)
    assert code == 0 and seen["capacity"] == RecoveryCapacityMeasurement(**_GOOD)
    assert _run_cli(cli, monkeypatch, tmp_path, {**_GOOD, "decode_events_per_second": 1_000})[0] == 3
    assert _run_cli(cli, monkeypatch, tmp_path, None)[0] == 0
    assert _run_cli(cli, monkeypatch, tmp_path, None, complete=False)[0] == 1

    with pytest.raises(SystemExit) as excinfo:
        _run_cli(cli, monkeypatch, tmp_path, {**_GOOD, "events_per_second": 0})
    assert excinfo.value.code == 2


def test_cli_replace_existing_and_projection_routing(cli, monkeypatch, tmp_path) -> None:
    order: list[str] = []
    monkeypatch.setattr(cli, "purge_origin_projections", lambda client, **kw: order.append(f"purge:{kw['service_id']}"))
    monkeypatch.setattr(
        cli,
        "rebuild_origin_projections_from_fos",
        lambda *a, **kw: order.append("rebuild") or RecoveryReport(True, True, True, True, True, 1, 1, (), "rb-1"),
    )
    monkeypatch.setattr("sys.argv", ["backfill", "--service-id", "svc", "--replace-existing"])
    assert cli.main() == 0 and order == ["purge:svc", "rebuild"]

    purged: list[str] = []
    monkeypatch.setattr(cli, "purge_origin_projections", lambda client, **kw: purged.append("purge"))
    _run_cli(cli, monkeypatch, tmp_path, None)
    assert purged == []

    monkeypatch.setattr("sys.argv", ["backfill", "--service-id", "svc", "--replace-existing", "--rebuild-id", "rb-1"])
    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 2

    calls, adapters = [], []
    monkeypatch.setattr(cli, "ClickHouseBatchAdapter", lambda client: adapters.append("facts") or MagicMock())
    monkeypatch.setattr(cli, "AggregateBatchAdapter", lambda client: adapters.append("aggregate") or MagicMock())
    monkeypatch.setattr(cli, "purge_cmcd_projections", lambda client, **kw: calls.append("purge-cmcd"))
    monkeypatch.setattr(cli, "purge_origin_projections", lambda client, **kw: calls.append("purge-origin"))
    monkeypatch.setattr(
        cli,
        "rebuild_cmcd_projections_from_fos",
        lambda *a, **kw: calls.append("rebuild-cmcd") or RecoveryReport(True, True, True, True, True, 1, 1, (), "rb-1"),
    )
    monkeypatch.setattr(
        cli,
        "rebuild_origin_projections_from_fos",
        lambda *a, **k: pytest.fail("origin rebuild must not run for --projection cmcd"),
    )
    monkeypatch.setattr("sys.argv", ["backfill", "--service-id", "svc", "--projection", "cmcd", "--replace-existing"])
    assert cli.main() == 0 and calls == ["purge-cmcd", "rebuild-cmcd"] and adapters == ["facts"]

    seen, rum_adapters = {}, []
    monkeypatch.setattr(cli, "ClickHouseBatchAdapter", lambda client: rum_adapters.append("facts") or MagicMock())
    monkeypatch.setattr(cli, "purge_rum_facts", lambda client, **kw: seen.setdefault("purge", kw))
    monkeypatch.setattr(
        cli,
        "rebuild_rum_facts_from_fos",
        lambda *a, **kw: (
            seen.setdefault("rebuild", kw) and RecoveryReport(True, True, True, True, True, 1, 1, (), "rb-1")
        ),
    )
    monkeypatch.setattr(
        "sys.argv", ["backfill", "--service-id", "svc", "--projection", "rum_errors", "--replace-existing"]
    )
    assert cli.main() == 0 and rum_adapters == ["facts"]
    assert seen["purge"]["domain"] == "rum_errors" and seen["purge"]["service_id"] == "svc"
    assert seen["rebuild"]["domain"] == "rum_errors" and seen["rebuild"]["service_id"] == "svc"


# ── Merged from test_archive_only_recovery.py ────────────────────────


class _MockClickHouseClient:
    def __init__(self) -> None:
        self.batches: list[HighScaleBatch] = []

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    @property
    def request_rows(self) -> list[dict[str, Any]]:
        return [r for b in self.batches if b.domain == "request" for r in b.rows]


class _InMemoryCatalog:
    def __init__(self, manifests: tuple[ArchiveManifest, ...]) -> None:
        self._manifests = manifests

    def manifests_covering(
        self, service_id: str, domain: str, start: datetime, end: datetime
    ) -> tuple[ArchiveManifest, ...]:
        return tuple(
            m
            for m in self._manifests
            if m.service_id == service_id and m.domain == domain and m.coverage_end > start and m.coverage_start < end
        )


def _build_synthetic_batch_archive(
    service_id: str, output_dir: Path, source_count: int = 5, rows_per_source: int = 5, now: datetime | None = None
) -> tuple[ArchiveManifest, bytes, list[dict[str, Any]]]:
    observed = now or datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
    source_batches: list[tuple[ArchiveSourceObject, tuple[dict[str, Any], ...]]] = []
    all_events: list[dict[str, Any]] = []
    for s_idx in range(source_count):
        events_for_source = [
            {
                "event_id": f"evt-{service_id}-{s_idx}-{r_idx}",
                "timestamp": (observed - timedelta(minutes=s_idx, seconds=r_idx * 10)).isoformat(),
                "client_ip": f"198.51.100.{(s_idx * 5 + r_idx) % 250 + 1}",
                "request_method": "GET" if r_idx % 2 == 0 else "POST",
                "url": f"/api/v1/item/{s_idx}_{r_idx}",
                "status": 200 if r_idx != 3 else 500,
                "ost": 200 if r_idx != 3 else 500,
                "ottfb": 45000.0,
                "ttfb": 0.045,
                "cache": "MISS",
                "resp_bytes": 1024 * (r_idx + 1),
                "pop": "JFK" if s_idx % 2 == 0 else "SFO",
                "fastly_backend_name": "origin-primary",
                "time_elapsed": 30000 + (r_idx * 500),
            }
            for r_idx in range(rows_per_source)
        ]
        all_events.extend(events_for_source)
        source_obj = ArchiveSourceObject(
            service_id=service_id,
            domain="request",
            object_key=f"raw/request/year=2026/month=10/day=09/source_{s_idx:03d}.gz",
            checksum=f"sha256:synth_{s_idx:04d}",
            size_bytes=512,
            version=f"v{s_idx}",
        )
        source_batches.append((source_obj, tuple(events_for_source)))

    manifest = write_batch_archive_checkpoint(
        output_dir,
        service_id=service_id,
        domain="request",
        source_batches=source_batches,
        archive_epoch=1,
        schema_version="request.v1",
        transform_version="normalize.v1",
        coverage_start=observed - timedelta(hours=1),
        coverage_end=observed,
        retention_seconds=86400 * 30,
        deletion_grace_seconds=86400 * 7,
    )
    artifact_bytes = Path(manifest.artifact.uri.removeprefix("file://")).read_bytes()
    normalized = replace(
        manifest, artifact=replace(manifest.artifact, uri=f"s3://archive/artifacts/{manifest.manifest_id}.parquet")
    )
    return normalized, artifact_bytes, all_events


def _setup_recovery_fixture(
    service_id: str, now: datetime
) -> tuple[ArchiveManifest, list[dict[str, Any]], ArchivePublication, _MockClickHouseClient, ClickHousePublication]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest, artifact_bytes, expected_events = _build_synthetic_batch_archive(service_id, Path(tmp_dir), 5, 5, now)
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(manifest, artifact_bytes)
    ch_client = _MockClickHouseClient()
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ch_client)
    return manifest, expected_events, archive, ch_client, publication


@pytest.mark.security_regression
def test_archive_only_recovery_rebuilds_all_events_without_raw_logs() -> None:
    manifest, expected_events, archive, ch_client, publication = _setup_recovery_fixture(
        "recover-svc-001", datetime(2026, 10, 9, 14, 0, tzinfo=UTC)
    )
    assert archive.is_replayable(manifest.manifest_id)
    replay_res = replay_manifest(manifest, archive, publication)
    assert (replay_res.manifest_id, replay_res.rows_read, replay_res.publication.rows_visible) == (
        manifest.manifest_id,
        25,
        25,
    )
    assert replay_res.publication.status == PublicationStatus.VISIBLE and replay_res.publication.duplicate is False
    assert {r["event_id"] for r in ch_client.request_rows} == {e["event_id"] for e in expected_events}


@pytest.mark.security_regression
def test_archive_only_recovery_rebuilds_origin_projections() -> None:
    manifest, _, archive, ch_client, publication = _setup_recovery_fixture(
        "recover-svc-002", datetime(2026, 10, 9, 14, 30, tzinfo=UTC)
    )
    proj_res = replay_origin_projections(manifest, archive, publication, rebuild_id="drill-origin-001")
    assert proj_res.rows_read == 25 and proj_res.rows_published > 0
    origin_batches = [b for b in ch_client.batches if b.domain in {"origin_summary", "origin_dimensions"}]
    assert len(origin_batches) >= 1 and sum(len(b.rows) for b in origin_batches) == proj_res.rows_published


@pytest.mark.security_regression
def test_archive_only_recovery_idempotency_and_rebuild_scopes() -> None:
    manifest, _, archive, ch_client, publication = _setup_recovery_fixture(
        "recover-svc-003", datetime(2026, 10, 9, 15, 0, tzinfo=UTC)
    )
    res1 = replay_manifest(manifest, archive, publication)
    assert res1.publication.duplicate is False and len(ch_client.batches) == 1
    res2 = replay_manifest(manifest, archive, publication)
    assert res2.publication.duplicate is True and len(ch_client.batches) == 1
    res3 = replay_manifest(manifest, archive, publication, batch_id=f"rebuild:disaster-drill-99:{manifest.manifest_id}")
    assert res3.publication.duplicate is False and len(ch_client.batches) == 2


@pytest.mark.security_regression
def test_archive_only_recovery_catalog_tiered_rebuild() -> None:
    service_id, now = "recover-svc-004", datetime(2026, 10, 9, 16, 0, tzinfo=UTC)
    with tempfile.TemporaryDirectory() as tmp_dir:
        recent_manifest, recent_bytes, _ = _build_synthetic_batch_archive(
            service_id, Path(tmp_dir), 3, 10, now - timedelta(hours=2)
        )
        warm_manifest, warm_bytes, _ = _build_synthetic_batch_archive(
            service_id, Path(tmp_dir), 4, 10, now - timedelta(days=5)
        )
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(recent_manifest, recent_bytes)
    archive.publish(warm_manifest, warm_bytes)
    catalog = _InMemoryCatalog((recent_manifest, warm_manifest))
    publication = ClickHousePublication(InMemoryBatchManifestStore(), _MockClickHouseClient())

    report = rebuild_serving_state_from_fos(
        catalog, archive, publication, service_id=service_id, domain="request", now=now
    )
    assert (report.full_rebuild_complete, report.manifests_replayed, report.rows_replayed) == (True, 2, 70)
    assert (
        report.triage_available and report.raw_query_available and report.warm_history_available and not report.errors
    )

    proj_report = rebuild_origin_projections_from_fos(catalog, archive, publication, service_id=service_id, now=now)
    assert proj_report.full_rebuild_complete and proj_report.manifests_replayed == 2 and proj_report.rows_replayed > 0


# ── Merged from tests/high_scale/test_console_access_integration.py ──
