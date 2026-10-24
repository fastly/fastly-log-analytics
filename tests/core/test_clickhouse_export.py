"""Bounded immutable Fastly Object Storage artifacts, without external I/O."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import duckdb
import pytest

from backend.core.clickhouse_export import FosArtifacts
from backend.core.clickhouse_manifest import Dataset
from backend.core.clickhouse_rows import canonical_row, digest_rows
from scripts import clickhouse_replay
from scripts.clickhouse_replay import parser, run
from tests.core.clickhouse_fos_fixture import TinyFos


def _rows_from_fixture(con) -> list[tuple]:
    out = []
    for idx, row in enumerate(con.execute("SELECT * FROM fixture").fetch_arrow_table().to_pylist()):
        row["row_ordinal"] = idx
        out.append(canonical_row(row))
    return out


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
    all_rows = _rows_from_fixture(con)
    artifacts = [objects.put(dataset, tuple(all_rows[i : i + 2])) for i in range(0, len(all_rows), 2)]
    rows = tuple(row for artifact in artifacts for row in objects.load(artifact).rows)
    assert [row[1] for row in rows] == list(range(5))
    assert digest_rows(rows) == digest_rows(tuple(all_rows))
    assert len(artifacts) == 3
    assert fos.puts == 3
    assert all("/clickhouse-prototype/" in a.artifact_uri for a in artifacts)
    assert rows[0][3] is None


def test_corrupt_artifact_or_wrong_bucket_rejected(fixture):
    dataset, objects, fos, con = fixture
    all_rows = _rows_from_fixture(con)
    artifact = objects.put(dataset, tuple(all_rows))
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
    all_rows = _rows_from_fixture(con)
    with pytest.raises(ValueError, match="HEAD"):
        objects.put(dataset, tuple(all_rows))


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
    for name in ("PgManifest", "FosArtifacts"):
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
