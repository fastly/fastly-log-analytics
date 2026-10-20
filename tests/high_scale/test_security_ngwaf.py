"""NGWAF bot enrichment feeds the facts rows, the Security projection and its archive rebuild."""

from __future__ import annotations

import gzip
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest

from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.ingest_controller import HighScaleIngestController, _enrich_high_scale_ngwaf_bots
from backend.high_scale.ledger import HighScaleLedger
from backend.high_scale.ownership import OwnershipStore
from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
)
from backend.high_scale.publication import _extract_custom_fields as persisted_fact_fields
from backend.high_scale.security_replay import rebuild_security_projections_from_fos, replay_security_projections

NOW = datetime(2026, 9, 11, 20, 0, tzinfo=UTC)
_PG_TARGET = "backend.core.metadata.pg_connection.get_pg_readonly_connection"
_BOT_ROW = {"waf_req_id": "req-1", "bot_name": "bingbot", "category": "search", "wellknown_bot_name": "bing"}


class _Pg:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def execute(self, sql: str, params: list[str]) -> _Pg:
        return self

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows

    def close(self) -> None:
        return None


class _ClickHouse:
    def __init__(self) -> None:
        self.batches: list[HighScaleBatch] = []

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    def rows(self, domain: str) -> list[dict[str, Any]]:
        return [row for b in self.batches if b.domain == domain for row in b.rows]


class _Catalog:
    def __init__(self, *manifests: Any) -> None:
        self._manifests = manifests

    def manifests_covering(self, service_id: str, domain: str, start: datetime, end: datetime) -> tuple[Any, ...]:
        return self._manifests


def _event(waf_req_id: str | None) -> dict[str, Any]:
    return {"timestamp": "2026-09-11T20:00:00Z", "url": "/ok", **({"waf_req_id": waf_req_id} if waf_req_id else {})}


def test_enrichment_tags_events_whose_waf_req_id_is_in_the_bot_cache():
    events = (_event("req-1"), _event("req-unknown"), _event(None))
    with patch(_PG_TARGET, return_value=_Pg([_BOT_ROW])):
        _enrich_high_scale_ngwaf_bots(events)
    tagged, unknown, bare = events
    assert tagged["_ngwaf_bot_name"] == "bingbot"
    assert tagged["ngwaf_bot_verification_state"] == "verified"
    assert persisted_fact_fields(tagged) == {
        "ngwaf_bot": "bingbot",
        "ngwaf_bot_category": "search",
        "ngwaf_wellknown_bot": "bing",
    }
    for untouched in (unknown, bare):
        assert not {k for k in untouched if "ngwaf" in k}


def test_enrichment_never_invents_bots_for_synthetic_looking_ids():
    events = (_event("waf-123"),)
    with patch(_PG_TARGET, return_value=_Pg([])):
        _enrich_high_scale_ngwaf_bots(events)
    assert not {k for k in events[0] if "ngwaf" in k}


def test_enrichment_failure_is_logged_not_silent(caplog: pytest.LogCaptureFixture):
    events = (_event("req-1"),)
    with patch(_PG_TARGET, side_effect=RuntimeError("pg down")), caplog.at_level("WARNING"):
        _enrich_high_scale_ngwaf_bots(events)
    assert "NGWAF bot enrichment failed" in caplog.text
    assert "_ngwaf_bot_name" not in events[0]


def _controller() -> tuple[HighScaleIngestController, InMemoryObjectStore, _ClickHouse, _ClickHouse]:
    ownership = OwnershipStore()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    objects = InMemoryObjectStore()
    facts = _ClickHouse()
    aggregates = _ClickHouse()
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=HighScaleLedger(),
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), facts),
        aggregates=ClickHousePublication(InMemoryBatchManifestStore(), aggregates),
        worker_id="worker-1",
        deletion_grace_seconds=60,
    )
    return controller, objects, facts, aggregates


def _ingest(controller: HighScaleIngestController) -> Any:
    payload = gzip.compress((str(_event("req-1")).replace("'", '"') + "\n").encode())
    return controller.ingest(
        service_id="svc",
        domain="request",
        object_key="raw/request/one.gz",
        checksum="sha256:source",
        payload=payload,
        version="v1",
        now=NOW,
    )


def test_ingest_persists_the_bot_on_the_fact_row_and_projects_it():
    controller, _, facts, aggregates = _controller()
    with patch(_PG_TARGET, return_value=_Pg([_BOT_ROW])):
        _ingest(controller)

    fact = facts.rows("request")[0]
    assert persisted_fact_fields(fact)["ngwaf_bot"] == "bingbot"
    projected = aggregates.rows("security_dimensions")
    assert [(r["dimension"], r["value"], r["requests"], r["verified_count"]) for r in projected] == [
        ("ngwaf_bot", "bingbot", 1, 1)
    ]
    assert projected[0]["wellknown_bot_name"] == "bing"
    assert projected[0]["bot_category"] == "search"


def test_archive_stays_raw_so_a_rebuild_can_enrich_against_the_current_cache():
    controller, objects, _, aggregates = _controller()
    with patch(_PG_TARGET, return_value=_Pg([])):
        result = _ingest(controller)
    assert aggregates.rows("security_dimensions") == []

    archive = ArchivePublication(objects)
    rebuilt = _ClickHouse()
    publication = ClickHousePublication(InMemoryBatchManifestStore(), rebuilt)
    with patch(_PG_TARGET, return_value=_Pg([_BOT_ROW])):
        replayed = replay_security_projections(result.manifest, archive, publication, rebuild_id="drill-1")

    assert (replayed.rows_read, replayed.rows_published) == (1, 1)
    assert [b.domain for b in rebuilt.batches] == ["security_dimensions"]
    assert rebuilt.batches[0].batch_id.startswith("rebuild-security:drill-1:")
    assert rebuilt.rows("security_dimensions")[0]["value"] == "bingbot"


def test_replay_publishes_nothing_when_no_event_resolves_to_a_bot():
    controller, objects, _, _ = _controller()
    with patch(_PG_TARGET, return_value=_Pg([])):
        result = _ingest(controller)
        replayed = replay_security_projections(
            result.manifest,
            ArchivePublication(objects),
            ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse()),
            rebuild_id="drill-2",
        )
    assert (replayed.rows_read, replayed.rows_published) == (1, 0)


def test_rebuild_from_fos_replays_every_request_manifest_and_resumes_idempotently():
    controller, objects, _, _ = _controller()
    with patch(_PG_TARGET, return_value=_Pg([])):
        result = _ingest(controller)
    archive = ArchivePublication(objects)
    rebuilt = _ClickHouse()
    publication = ClickHousePublication(InMemoryBatchManifestStore(), rebuilt)

    with patch(_PG_TARGET, return_value=_Pg([_BOT_ROW])):
        first = rebuild_security_projections_from_fos(
            _Catalog(result.manifest), archive, publication, service_id="svc", now=NOW + timedelta(hours=1)
        )
        second = rebuild_security_projections_from_fos(
            _Catalog(result.manifest),
            archive,
            publication,
            service_id="svc",
            now=NOW + timedelta(hours=1),
            rebuild_id=first.rebuild_id,
        )

    assert first.full_rebuild_complete is True
    assert first.manifests_replayed == second.manifests_replayed == 1
    assert first.rows_replayed == second.rows_replayed == 1
    assert len(rebuilt.batches) == 1


def test_rebuild_reports_a_failed_manifest_instead_of_hiding_it():
    controller, objects, _, _ = _controller()
    with patch(_PG_TARGET, return_value=_Pg([])):
        result = _ingest(controller)
    report = rebuild_security_projections_from_fos(
        _Catalog(result.manifest),
        ArchivePublication(InMemoryObjectStore()),
        ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse()),
        service_id="svc",
        now=NOW + timedelta(hours=1),
    )
    assert report.full_rebuild_complete is False
    assert report.manifests_replayed == 0
    assert "failed to rebuild Security projections" in report.errors[0]


def test_rebuild_rejects_a_non_positive_window():
    with pytest.raises(ValueError, match="window must be positive"):
        rebuild_security_projections_from_fos(
            _Catalog(),
            ArchivePublication(InMemoryObjectStore()),
            ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse()),
            service_id="svc",
            window=timedelta(0),
        )
