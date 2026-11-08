"""Superseding orphaned high-scale publication manifests (Postgres bookkeeping only)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from backend.high_scale import publication_reaper
from backend.high_scale.archive import (
    BatchManifest,
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    PublicationStatus,
)
from backend.high_scale.archive.clickhouse_batch import SupersededPublication, publication_batch_uuid
from backend.high_scale.archive.manifests import build_batch_id_from_manifest_id
from backend.high_scale.postgres_control import HighScaleDeletionSweeper, PostgresControlPlane
from backend.high_scale.publication_reaper import PublicationReaper, clickhouse_visible_receipts, publication_batch_id


def _no_receipts(_service_id: str, _batch_uuids: frozenset[str]) -> set[str]:
    return set()


class _Seed:
    def __init__(self, control: PostgresControlPlane, sid: str) -> None:
        self.control, self.sid = control, sid

    def archive(
        self, manifest_id: str, domain: str, *, state: str = "manifest_committed", age_hours: float = 2
    ) -> None:
        with self.control.transaction() as connection:
            connection.execute(
                "INSERT INTO high_scale_archive_manifests (manifest_id, service_id, domain, source_key, source_checksum, "
                "source_size, artifact_uri, artifact_checksum, artifact_size, row_count, byte_count, canonical_digest, "
                "schema_version, transform_version, coverage_start, coverage_end, retention_deadline, "
                "deletion_authorization_deadline, archive_epoch, owner_epoch, state, created_at) VALUES "
                "(%s, %s, %s, 'raw/k', 'c', 1, %s, 'c', 1, 1, 1, 'd', 'v', 'v', clock_timestamp(), clock_timestamp(), "
                "clock_timestamp(), clock_timestamp(), 0, 1, %s, clock_timestamp() - make_interval(secs => %s))",
                (manifest_id, self.sid, domain, f"s3://a/{manifest_id}.parquet", state, age_hours * 3600),
            )

    def source(
        self, manifest_id: str, key: str, *, status: str, owner_manifest: str | None, domain: str = "request"
    ) -> None:
        with self.control.transaction() as connection:
            connection.execute(
                "INSERT INTO high_scale_archive_manifest_sources (manifest_id, source_key, source_checksum, source_size, "
                "row_start, row_count, deletion_deadline) VALUES (%s, %s, 'c', 1, 0, 1, clock_timestamp())",
                (manifest_id, key),
            )
            connection.execute(
                "INSERT INTO high_scale_source_objects (object_id, service_id, domain, object_key, checksum, size_bytes, "
                "status, archive_manifest_id) VALUES (%s, %s, %s, %s, 'c', 1, %s, %s) "
                "ON CONFLICT (service_id, object_key) DO NOTHING",
                (f"{self.sid}:{key}", self.sid, domain, key, status, owner_manifest),
            )

    def publication(self, batch_id: str, domain: str, *, status: str, age_hours: float = 2, rows: int = 5) -> None:
        with self.control.transaction() as connection:
            connection.execute(
                "INSERT INTO high_scale_publication_manifests (batch_id, service_id, domain, generation, batch_digest, "
                "expected_rows, visible_rows, status, updated_at) VALUES (%s, %s, %s, '1', 'sha256:x', %s, %s, %s, "
                "clock_timestamp() - make_interval(secs => %s))",
                (batch_id, self.sid, domain, rows, rows if status == "visible" else 0, status, age_hours * 3600),
            )

    def status(self, batch_id: str) -> str:
        with self.control.transaction(read_only=True) as connection:
            return connection.execute(
                "SELECT status FROM high_scale_publication_manifests WHERE batch_id=%s", (batch_id,)
            ).fetchone()[0]

    def cleanup(self) -> None:
        with self.control.transaction() as connection:
            connection.execute("DELETE FROM high_scale_publication_manifests WHERE service_id=%s", (self.sid,))
            connection.execute("DELETE FROM high_scale_source_objects WHERE service_id=%s", (self.sid,))
            connection.execute("DELETE FROM high_scale_archive_manifests WHERE service_id=%s", (self.sid,))


@pytest.fixture
def seed(request):
    control = PostgresControlPlane()
    control.create_schema()
    s = _Seed(control, f"svc-reaper-{request.node.name}"[:60])
    s.cleanup()
    yield s
    s.cleanup()


def _superseded_retry(seed: _Seed, orphan: str, replacement: str, domain: str = "request") -> tuple[str, str]:
    """An orphaned attempt whose only source was later acknowledged under ``replacement``."""
    seed.archive(orphan, "request")
    seed.archive(replacement, "request", state="source_deleted", age_hours=1.5)
    seed.source(orphan, f"raw/{orphan}", status="acknowledged", owner_manifest=replacement)
    seed.source(replacement, f"raw/{orphan}", status="acknowledged", owner_manifest=replacement)
    pending = publication_batch_id(orphan, "request", domain)
    seed.publication(pending, domain, status="pending")
    seed.publication(publication_batch_id(replacement, "request", domain), domain, status="visible")
    return pending, publication_batch_id(replacement, "request", domain)


def test_publication_batch_id_follows_coordinator_naming() -> None:
    assert publication_batch_id("m1", "request", "request") == build_batch_id_from_manifest_id("m1")
    assert publication_batch_id("m1", "request", "cmcd") == build_batch_id_from_manifest_id("m1:cmcd")
    assert publication_batch_id("m1", "rum_vitals", "rum_errors") == build_batch_id_from_manifest_id("m1:rum_errors")


def test_publish_refuses_a_superseded_manifest_so_a_late_retry_cannot_double_count() -> None:
    store = InMemoryBatchManifestStore()
    batch = HighScaleBatch("b1", "svc", "request", "1", ({"event_id": "e1"},))
    store.put(BatchManifest("b1", "svc", "request", "1", batch.digest, 1, PublicationStatus.SUPERSEDED))
    client = MagicMock()
    with pytest.raises(SupersededPublication):
        ClickHousePublication(store, client).publish(batch)
    client.insert.assert_not_called()


def test_superseded_status_is_accepted_by_existing_schema_and_round_trips(seed) -> None:
    with seed.control.transaction() as connection:
        connection.execute(
            "ALTER TABLE high_scale_publication_manifests DROP CONSTRAINT high_scale_publication_manifests_status_check"
        )
        connection.execute(
            "ALTER TABLE high_scale_publication_manifests ADD CONSTRAINT high_scale_publication_manifests_status_check "
            "CHECK (status IN ('pending', 'visible'))"
        )
    seed.control.create_schema()
    seed.control.create_schema()
    seed.publication("b-sup", "request", status="superseded")
    manifest = seed.control.get_batch_manifest("b-sup")
    assert manifest is not None and manifest.status is PublicationStatus.SUPERSEDED
    publications = {p["domain"]: p for p in seed.control.operational_snapshot(seed.sid)["publications"]}
    assert publications["request"]["pending"] == 0 and publications["request"]["lag_seconds"] == 0


def test_reaper_supersedes_orphan_only_on_apply_and_is_idempotent(seed) -> None:
    pending, replacement = _superseded_retry(seed, "orphan-1", "replacement-1")
    agg_pending, _ = _superseded_retry(seed, "orphan-2", "replacement-2", domain="request_aggregate")
    reaper = PublicationReaper(control=seed.control, visible_receipts=_no_receipts)

    dry = reaper.reap(seed.sid, apply=False)
    assert set(dry.superseded) == {pending, agg_pending} and dry.applied == 0
    assert seed.status(pending) == "pending" and seed.status(agg_pending) == "pending"

    applied = reaper.reap(seed.sid, apply=True)
    assert applied.applied == 2
    assert seed.status(pending) == "superseded" and seed.status(agg_pending) == "superseded"
    assert seed.status(replacement) == "visible"
    again = reaper.reap(seed.sid, apply=True)
    assert again.superseded == () and again.applied == 0


def test_reaper_keeps_every_row_it_cannot_prove_was_republished(seed) -> None:
    proven, _ = _superseded_retry(seed, "ok", "ok-rep")

    def orphan(name: str, status: str, owner: str | None, *, age_hours: float = 2) -> str:
        seed.archive(name, "request", age_hours=age_hours)
        seed.source(name, f"raw/{name}", status=status, owner_manifest=owner)
        batch_id = publication_batch_id(name, "request", "request")
        seed.publication(batch_id, "request", status="pending", age_hours=age_hours)
        return batch_id

    seed.archive("rep", "request", state="source_deleted")
    seed.publication(publication_batch_id("rep", "request", "request"), "request", status="visible")
    missing = orphan("missing", "source_missing", None)
    retrying = orphan("retrying", "discovered", None)
    own = orphan("own", "acknowledged", "own")
    young = orphan("young", "acknowledged", "rep", age_hours=0.1)
    seed.archive("rep-pending", "request", state="source_deleted")
    seed.publication(publication_batch_id("rep-pending", "request", "request"), "request", status="pending")
    rep_pending = orphan("rep-not-visible", "acknowledged", "rep-pending")
    seed.archive("rep-absent", "request", state="source_deleted")
    rep_absent = orphan("rep-without-publication", "acknowledged", "rep-absent")
    receipt = orphan("receipt-visible", "acknowledged", "rep")
    receipt_uuid = publication_batch_uuid(seed.sid, "request", "1", receipt)

    reaper = PublicationReaper(control=seed.control, visible_receipts=lambda _s, uuids: {receipt_uuid} & set(uuids))
    result = reaper.reap(seed.sid, apply=True)

    assert result.superseded == (proven,)
    for kept in (missing, retrying, own, young, rep_pending, rep_absent, receipt):
        assert seed.status(kept) == "pending", kept
    assert result.kept == {
        "sources_not_republished": 3,
        "replacement_not_visible": 2,
        "receipt_visible": 1,
    }


def test_reaper_supersedes_rum_sibling_when_some_replacements_had_no_rows_for_that_domain(seed) -> None:
    seed.archive("rum-orphan", "rum_vitals")
    seed.archive("rum-rep-a", "rum_vitals", state="source_deleted")
    seed.archive("rum-rep-b", "rum_vitals", state="source_deleted")
    seed.source("rum-orphan", "raw/rum/a", status="acknowledged", owner_manifest="rum-rep-a", domain="rum_vitals")
    seed.source("rum-orphan", "raw/rum/b", status="source_deleted", owner_manifest="rum-rep-b", domain="rum_vitals")
    sibling = publication_batch_id("rum-orphan", "rum_vitals", "rum_errors")
    seed.publication(sibling, "rum_errors", status="pending")
    seed.publication(publication_batch_id("rum-rep-a", "rum_vitals", "rum_errors"), "rum_errors", status="visible")

    result = PublicationReaper(control=seed.control, visible_receipts=_no_receipts).reap(seed.sid, apply=True)

    assert result.superseded == (sibling,) and seed.status(sibling) == "superseded"


def test_reaper_max_age_window_and_pages_cover_every_orphan(seed) -> None:
    recent = [_superseded_retry(seed, f"recent-{i}", f"recent-rep-{i}")[0] for i in range(3)]
    old, _ = _superseded_retry(seed, "old", "old-rep")
    with seed.control.transaction() as connection:
        connection.execute(
            "UPDATE high_scale_archive_manifests SET created_at = clock_timestamp() - interval '3 days' "
            "WHERE manifest_id = 'old'"
        )
    reaper = PublicationReaper(control=seed.control, visible_receipts=_no_receipts)

    first = reaper.reap(seed.sid, apply=False, max_age_seconds=86400, limit=2)
    second = reaper.reap(seed.sid, apply=False, max_age_seconds=86400, limit=2, after=first.cursor)
    assert first.cursor is not None and second.cursor is None
    assert set(first.superseded) | set(second.superseded) == set(recent)

    backlog = reaper.reap(seed.sid, apply=False)
    assert set(backlog.superseded) == {*recent, old}


def test_mark_superseded_never_overwrites_a_publication_that_became_visible(seed) -> None:
    seed.publication("raced", "request", status="visible")
    seed.publication("stuck", "request", status="pending")
    assert seed.control.mark_publications_superseded(seed.sid, ("raced", "stuck")) == 1
    assert seed.status("raced") == "visible" and seed.status("stuck") == "superseded"


def test_automatic_reaping_is_throttled_and_bounded_to_recent_orphans() -> None:
    clock = iter([1000.0, 1100.0, 1700.0])
    reaper = PublicationReaper(control=MagicMock(), visible_receipts=_no_receipts, clock=lambda: next(clock))
    reaper.reap = MagicMock(return_value=publication_reaper.ReapResult((), {}, 0, 0, None))  # type: ignore[method-assign]
    for _ in range(3):
        reaper.maybe_reap("svc")
    assert reaper.reap.call_count == 2
    kwargs = reaper.reap.call_args.kwargs
    assert kwargs["apply"] is True and kwargs["max_age_seconds"] == reaper.max_age_seconds > 0


def test_deletion_sweeper_runs_the_publication_reaper_and_survives_its_failure() -> None:
    control = MagicMock()
    control.deletable_sources.return_value = ()
    reaper = MagicMock()
    reaper.maybe_reap.side_effect = RuntimeError("pg down")
    sweeper = HighScaleDeletionSweeper(control=control, controller=MagicMock(), publication_reaper=reaper)
    sweeper.sweep(service_id="svc")
    reaper.maybe_reap.assert_called_once_with("svc")


def test_clickhouse_visible_receipts_inlines_only_validated_uuids() -> None:
    client = MagicMock()
    good = "b63eba3a-45ec-1679-9355-76ae6be2d4ee"
    client.execute.return_value = [{"batch_id": good}]
    check = clickhouse_visible_receipts(client)
    assert check("svc", frozenset({good})) == {good}
    sql, params = client.execute.call_args.args
    assert f"'{good}'" in sql and "publication_state='visible'" in sql and params == {"service_id": "svc"}
    with pytest.raises(ValueError):
        check("svc", frozenset({"x') OR 1=1 --"}))
    client.execute.reset_mock()
    assert check("svc", frozenset()) == set()
    client.execute.assert_not_called()


def test_cli_is_a_dry_run_unless_apply_is_given(seed, monkeypatch, capsys) -> None:
    pending, _ = _superseded_retry(seed, "cli-orphan", "cli-rep")
    monkeypatch.setattr(publication_reaper, "_visible_receipts_from_environment", lambda: _no_receipts)

    assert publication_reaper.main(["--service-id", seed.sid]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["apply"] is False and report["would_supersede"] == {"request": 1} and report["applied"] == 0
    assert seed.status(pending) == "pending"

    assert publication_reaper.main(["--service-id", seed.sid, "--apply"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["applied"] == 1 and seed.status(pending) == "superseded"
