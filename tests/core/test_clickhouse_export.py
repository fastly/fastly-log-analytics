"""Bounded immutable Fastly Object Storage artifacts, without external I/O."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import duckdb
import pytest

from backend.core.clickhouse_export import FosArtifacts, expiry_for, export_reader, pinned_reader
from backend.core.clickhouse_manifest import Dataset
from backend.core.clickhouse_rows import digest_rows
from scripts import clickhouse_replay
from scripts.clickhouse_replay import parser, run
from tests.core.clickhouse_fos_fixture import TinyFos


@pytest.fixture
def fixture():
    source = {"service_id": "TestPrototype", "name": "TestPrototype", "bucket": "test-bucket", "prefix": "logs"}
    fos = TinyFos()
    objects = FosArtifacts(source, client=fos)
    dataset = Dataset(
        source["service_id"],
        "test-dataset",
        "test-catalog",
        "logs_fixture",
        1,
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime.now(UTC) + timedelta(hours=1),
        0,
        digest_rows(()),
    )
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE fixture AS SELECT 'source' AS source_identity, "
        "'2026-09-01 00:00:00'::TIMESTAMP AS timestamp, NULL::VARCHAR AS country, "
        "'192.0.2.1' AS ip, '/' AS url, 1::BIGINT AS conn_requests FROM range(5)"
    )
    yield dataset, objects, fos, con
    con.close()


def test_export_roundtrip_stable_ordinals_and_duplicates(fixture):
    dataset, objects, fos, con = fixture
    store = MagicMock()
    result = export_reader(
        con.execute("SELECT * FROM fixture").fetch_record_batch(2),
        dataset=dataset,
        objects=objects,
        store=store,
        batch_rows=2,
    )
    assert result.expected_rows == 5
    sealed, artifacts = store.seal.call_args.args
    rows = tuple(row for artifact in artifacts for row in objects.load(artifact).rows)
    assert [row[1] for row in rows] == list(range(5))
    assert sealed.canonical_digest == digest_rows(rows)
    assert len(artifacts) == 3
    assert fos.puts == 3
    assert all("/clickhouse-prototype/" in a.artifact_uri for a in artifacts)
    assert rows[0][3] is None


def test_row_limit_and_oversized_row_never_seal(fixture):
    dataset, objects, _, con = fixture
    store = MagicMock()
    with pytest.raises(ValueError, match="row limit"):
        export_reader(
            con.execute("SELECT * FROM fixture").fetch_record_batch(2),
            dataset=dataset,
            objects=objects,
            store=store,
            max_rows=3,
            batch_rows=2,
        )
    store.seal.assert_not_called()
    con.execute("UPDATE fixture SET url = repeat('x', 70000)")
    with pytest.raises(ValueError, match="oversized row"):
        export_reader(
            con.execute("SELECT * FROM fixture").fetch_record_batch(2), dataset=dataset, objects=objects, store=store
        )
    store.seal.assert_not_called()


def test_corrupt_artifact_or_wrong_bucket_rejected(fixture):
    dataset, objects, fos, con = fixture
    store = MagicMock()
    export_reader(
        con.execute("SELECT * FROM fixture").fetch_record_batch(2), dataset=dataset, objects=objects, store=store
    )
    artifact = store.seal.call_args.args[1][0]
    with pytest.raises(ValueError, match="bucket"):
        objects.load(replace(artifact, artifact_uri=artifact.artifact_uri.replace("test-bucket", "other-bucket")))
    key = next(iter(fos.objects))
    payload, metadata = fos.objects[key]
    fos.objects[key] = (b"x" + payload[1:], metadata)
    with pytest.raises(ValueError, match="hash"):
        objects.load(artifact)


def test_head_mismatch_never_seals(fixture):
    dataset, objects, fos, con = fixture
    fos.head_object = lambda **kwargs: {"ContentLength": 0, "Metadata": {}}
    store = MagicMock()
    with pytest.raises(ValueError, match="HEAD"):
        export_reader(
            con.execute("SELECT * FROM fixture").fetch_record_batch(2), dataset=dataset, objects=objects, store=store
        )
    store.seal.assert_not_called()


def test_customer_retention_caps_expiry():
    now = datetime(2026, 9, 7, tzinfo=UTC)
    assert expiry_for(now - timedelta(days=29, hours=12), retention_days=30, now=now) == now + timedelta(hours=12)
    assert expiry_for(now, retention_days=0, now=now) == now + timedelta(days=1)
    with pytest.raises(ValueError, match="retention"):
        expiry_for(now - timedelta(days=31), retention_days=30, now=now)
    with pytest.raises(ValueError, match="TTL"):
        expiry_for(now, retention_days=30, now=now, ttl_hours=25)


def test_snapshot_reader_is_single_bounded_scan():
    con = MagicMock()
    con.execute.return_value.description = [
        ("timestamp",),
        ("country",),
        ("ip",),
        ("url",),
        ("conn_requests",),
        ("source_file",),
    ]
    pinned_reader(
        con,
        {"service_id": "TestPrototype"},
        snapshot=7,
        start=datetime(2026, 9, 1, tzinfo=UTC),
        end=datetime(2026, 9, 2, tzinfo=UTC),
        max_rows=5,
    )
    sql, params = con.execute.call_args.args
    assert "AT (VERSION => 7)" in sql
    assert "ORDER BY timestamp, country, ip, url, conn_requests, source_identity" in sql
    assert "timestamp <= ?" in sql
    assert "LIMIT ?" in sql and params[-1] == 6
    assert "OFFSET" not in sql


def test_snapshot_reader_preserves_ingest_source_file_column():
    con = MagicMock()
    con.execute.return_value.description = [
        ("timestamp",),
        ("country",),
        ("ip",),
        ("url",),
        ("conn_requests",),
        ("_source_file",),
    ]
    pinned_reader(
        con,
        {"service_id": "TestPrototype"},
        snapshot=7,
        start=datetime(2026, 9, 1, tzinfo=UTC),
        end=datetime(2026, 9, 2, tzinfo=UTC),
    )
    assert "COALESCE(CAST(_source_file AS VARCHAR), '') AS source_identity" in con.execute.call_args.args[0]


def test_actual_ducklake_snapshot_survives_later_commit_and_keeps_end_boundary():
    with duckdb.connect() as con:
        con.execute("LOAD ducklake")
        # All rows are catalog-inlined: no disk catalog, parquet or temp files.
        con.execute("ATTACH 'ducklake::memory:' AS lake (DATA_PATH 'data/clickhouse-test-never-flushed')")
        con.execute(
            "CREATE TABLE lake.logs_testprototype(timestamp TIMESTAMP, country VARCHAR, "
            "ip VARCHAR, url VARCHAR, conn_requests BIGINT)"
        )
        con.execute(
            "INSERT INTO lake.logs_testprototype VALUES "
            "('2026-09-01',NULL,'192.0.2.1','/',1), ('2026-09-02','','192.0.2.1','/',1)"
        )
        snapshot = con.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()[0]
        con.execute("INSERT INTO lake.logs_testprototype VALUES ('2026-09-01','NEW','192.0.2.1','/',1)")
        rows = pinned_reader(
            con,
            {"service_id": "TestPrototype"},
            snapshot=snapshot,
            start=datetime(2026, 9, 1, tzinfo=UTC),
            end=datetime(2026, 9, 2, tzinfo=UTC),
        ).read_all()
        assert rows.num_rows == 2
        assert rows["country"].to_pylist() == [None, ""]
        assert rows["source_identity"].to_pylist() == ["", ""]


@pytest.mark.parametrize(
    "mode",
    [
        ["--export", "--start", "2026-09-01", "--end", "2026-09-02"],
        ["--rebuild", "dataset"],
        ["--resume", "generation"],
    ],
)
def test_cli_dryrun_never_opens_storage(monkeypatch, mode):
    no_storage = MagicMock(side_effect=AssertionError("must not open storage"))
    for name in ("PgManifest", "FosArtifacts", "export_snapshot"):
        monkeypatch.setattr(f"scripts.clickhouse_replay.{name}", no_storage)
    args = parser().parse_args(["--service-id", "TestPrototype", *mode])
    assert run(args)["dry_run"]
    no_storage.assert_not_called()


@pytest.mark.parametrize("extra", [["--limit", "0"], ["--max-rows", "1000001"], ["--ttl-hours", "nan"]])
def test_cli_rejects_unbounded_arguments(extra):
    args = parser().parse_args(
        ["--service-id", "TestPrototype", "--export", "--start", "2026-09-01", "--end", "2026-09-02", *extra]
    )
    with pytest.raises(ValueError):
        run(args)


def test_cli_flushes_metrics_on_success_and_failure(monkeypatch):
    events = []
    monkeypatch.setattr("sys.argv", ["replay", "--service-id", "TestPrototype", "--rebuild", "fixture"])
    monkeypatch.setattr(clickhouse_replay, "force_flush", lambda: events.append("flush"))
    monkeypatch.setattr(clickhouse_replay, "close_clickhouse_client", lambda: events.append("close"))
    monkeypatch.setattr(clickhouse_replay, "run", lambda args: {"dry_run": True})
    assert clickhouse_replay.main() == 0
    assert events == ["flush", "close"]
    events.clear()

    def fail(args):
        raise ValueError("PRIVATE")

    monkeypatch.setattr(clickhouse_replay, "run", fail)
    assert clickhouse_replay.main() == 1
    assert events == ["flush", "close"]
