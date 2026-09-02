"""``convert_batch_objects`` failure/edge paths.

The batched convert shares ONE DuckLake transaction and one ledger sweep
across N keys, which makes its failure semantics the interesting part:

* a whole-batch failure must make EVERY key it still owns retryable — a key
  left ``claimed`` is stranded until the reclaim timeout, and a key marked
  ``committed`` on a rolled-back transaction loses its rows for good once
  ``finalize_committed_raw`` deletes the raw file;
* one dead key must not strand its siblings — the dead-vs-transient split is
  applied per key BEFORE the shared transaction opens;
* the per-file bookkeeping (``ingested_files`` row counts, the quarantine
  pass) must stay per file: the batch total would break log-line-accounting
  reconciliation and point the operator at the wrong raw file.

Companion to tests/core/test_convert_batch.py, which owns the happy path and
the one-snapshot-per-batch contract. Real file-backed ``ducklake:`` attach,
real SQLite ledger; only the FOS/S3 client is mocked.
"""

import json
import time
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import duckdb

from backend.core.ingest import convert_batch_objects
from backend.core.metadata.base import get_con
from backend.core.metadata.quarantine import list_quarantined_files

BUCKET = "test-bucket"


def _clear_ledger(service_id: str):
    con = get_con(service_id)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id=?", (service_id,))
    con.execute("DELETE FROM quarantined_files")
    con.commit()
    return con, cur


def _seed(con, cur, service_id: str, keys: list[str], status: str = "discovered") -> None:
    for key in keys:
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, ?, ?)",
            (service_id, key, status, time.time()),
        )
    con.commit()


def _rows(con, service_id: str) -> dict[str, tuple]:
    return {
        r["object_key"]: (r["status"], r["attempts"], r["last_error"], r["committed_at"])
        for r in con.execute(
            "SELECT object_key, status, attempts, last_error, committed_at FROM ingest_ledger WHERE service_id=?",
            (service_id,),
        ).fetchall()
    }


def _write_files(tmp_path, names: list[str], extra: dict | None = None) -> tuple[list[str], dict[str, str]]:
    keys: list[str] = []
    s3_to_local: dict[str, str] = {}
    for i, name in enumerate(names):
        record = {"timestamp": f"2026-08-27T16:{i:02d}:00Z", "url": f"/{name}"}
        record.update(extra or {})
        local = tmp_path / f"{name}.json"
        local.write_text(json.dumps(record) + "\n")
        key = f"raw/2026/08/27/16/{i:02d}/{name}.json.gz"
        keys.append(key)
        s3_to_local[f"s3://{BUCKET}/{key}"] = str(local)
    return keys, s3_to_local


def _download_stub(mapping: dict[str, str]):
    def _download(fos_client, s3_paths, tmpdir):
        got = {p: mapping[p] for p in s3_paths if p in mapping}
        return got, {v: k for k, v in got.items()}

    return _download


def _attacher(catalog: str, data_path: str):
    def _attach(con_arg, src_arg, read_only=False):
        con_arg.execute("INSTALL ducklake; LOAD ducklake;")
        ro = ", READ_ONLY" if read_only else ""
        try:
            con_arg.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{data_path}'{ro})")
        except duckdb.Error:
            pass
        return True

    return _attach


@contextmanager
def _batch_env(service_id: str, tmp_path, download, fos=None, attach=None, cfg=None):
    src = {"service_id": service_id, "name": service_id, "bucket": BUCKET, "prefix": ""}
    real_attach = _attacher(str(tmp_path / "cat.ducklake"), str(tmp_path / "lakedata"))
    with (
        patch("backend.config.load_config", return_value=cfg or {"service_id": service_id}),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.ingest._get_fos_client", return_value=fos if fos is not None else MagicMock()),
        patch("backend.core.ingest._download_chunk_to_local", side_effect=download),
        patch(
            "backend.core.iceberg._ducklake._ducklake_attach",
            side_effect=attach if attach is not None else real_attach,
        ),
        patch("backend.core.ingest._configure_fos"),
        patch("backend.core.iceberg._ducklake.ducklake_table_name", return_value="logs"),
    ):
        yield real_attach


def _lake_reader(attach):
    con = duckdb.connect()
    attach(con, None, read_only=True)
    return con


# ── nothing to do ─────────────────────────────────────────────────────────


def test_convert_batch_with_an_empty_key_list_does_nothing():
    """The dispatcher slices keys into batches; an empty slice must not open
    a DuckLake connection or a catalog transaction."""
    service_id = "test-batch-edges-empty"
    _clear_ledger(service_id)

    with patch("backend.core.duckdb.get_source_for_service") as mock_src:
        summary = convert_batch_objects(service_id, [], "w1")

    mock_src.assert_not_called()
    assert summary == {
        "requested": 0,
        "claimed": 0,
        "not_claimed": 0,
        "committed": 0,
        "dead_letter": 0,
        "failed": 0,
    }


def test_convert_batch_claiming_nothing_returns_early(tmp_path):
    """Every key already claimed/committed elsewhere (a redelivered batch
    another worker won) must be a clean no-op, reported as not_claimed."""
    service_id = "test-batch-edges-noclaim"
    con, cur = _clear_ledger(service_id)
    keys, _ = _write_files(tmp_path, ["nc1", "nc2"])
    _seed(con, cur, service_id, keys, status="committed")

    with patch("backend.core.duckdb.get_source_for_service") as mock_src:
        summary = convert_batch_objects(service_id, keys, "w1")

    mock_src.assert_not_called()
    assert summary["requested"] == 2
    assert summary["claimed"] == 0
    assert summary["not_claimed"] == 2
    assert summary["committed"] == 0
    assert {v[0] for v in _rows(con, service_id).values()} == {"committed"}


# ── whole-batch failures make every key retryable ─────────────────────────


def test_convert_batch_without_a_registered_source_requeues_every_claimed_key(tmp_path):
    """The keys are already ``claimed`` at this point — returning without
    resetting them would strand all of them until the reclaim timeout."""
    service_id = "test-batch-edges-nosrc"
    con, cur = _clear_ledger(service_id)
    keys, _ = _write_files(tmp_path, ["ns1", "ns2", "ns3"])
    _seed(con, cur, service_id, keys)

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        summary = convert_batch_objects(service_id, keys, "w1")

    assert summary["claimed"] == 3
    assert summary["failed"] == 3
    assert summary["committed"] == 0
    for key, (status, attempts, last_error, committed_at) in _rows(con, service_id).items():
        assert status == "discovered", key
        assert attempts == 1
        assert "no source registered" in last_error
        assert committed_at is None


def test_convert_batch_attach_failure_requeues_the_whole_batch(tmp_path):
    """The transaction is shared, so its failure is the whole batch's — no
    key may be left claimed, committed, or with a stale attempt count."""
    service_id = "test-batch-edges-noattach"
    con, cur = _clear_ledger(service_id)
    keys, s3_map = _write_files(tmp_path, ["at1", "at2"])
    _seed(con, cur, service_id, keys)

    with _batch_env(service_id, tmp_path, _download_stub(s3_map), attach=lambda *a, **kw: False):
        summary = convert_batch_objects(service_id, keys, "w1")

    assert summary["claimed"] == 2
    assert summary["failed"] == 2
    assert summary["committed"] == 0
    for key, (status, attempts, last_error, _committed) in _rows(con, service_id).items():
        assert status == "discovered", key
        assert attempts == 1
        assert "attach failed" in last_error


def test_convert_batch_commits_even_when_ingested_files_bookkeeping_fails(tmp_path):
    """``insert_ingested_files`` runs inside the try block, so it must be
    best-effort: a metadata failure after the lake write must not undo the
    committed batch."""
    service_id = "test-batch-edges-bookkeeping"
    con, cur = _clear_ledger(service_id)
    keys, s3_map = _write_files(tmp_path, ["bk1", "bk2"])
    _seed(con, cur, service_id, keys)

    with _batch_env(service_id, tmp_path, _download_stub(s3_map)) as attach:
        with patch(
            "backend.core.ingest.metadata_db.insert_ingested_files",
            side_effect=Exception("metadata db locked"),
        ):
            summary = convert_batch_objects(service_id, keys, "w1")

    assert summary["committed"] == 2
    assert summary["failed"] == 0
    assert {v[0] for v in _rows(con, service_id).values()} == {"committed"}
    reader = _lake_reader(attach)
    assert reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 2
    reader.close()


# ── per-key download failures ─────────────────────────────────────────────


def test_convert_batch_transient_download_failure_does_not_strand_siblings(tmp_path):
    """One key whose object still HEADs fine gets requeued; the rest of the
    batch commits in the same run. Failing the whole batch for one blip
    would re-download the siblings on every retry."""
    service_id = "test-batch-edges-transient"
    con, cur = _clear_ledger(service_id)
    keys, s3_map = _write_files(tmp_path, ["ok1", "blip", "ok2"])
    _seed(con, cur, service_id, keys)

    blip_key = keys[1]
    del s3_map[f"s3://{BUCKET}/{blip_key}"]

    fos = MagicMock()
    fos.head_object.return_value = {"ContentLength": 5}
    with _batch_env(service_id, tmp_path, _download_stub(s3_map), fos=fos) as attach:
        summary = convert_batch_objects(service_id, keys, "w1")

    assert summary["claimed"] == 3
    assert summary["committed"] == 2
    assert summary["failed"] == 1
    assert summary["dead_letter"] == 0

    rows = _rows(con, service_id)
    assert rows[blip_key][0] == "discovered"
    assert rows[blip_key][1] == 1
    assert "transient" in rows[blip_key][2]
    assert rows[keys[0]][0] == "committed"
    assert rows[keys[2]][0] == "committed"

    reader = _lake_reader(attach)
    sources = {r[0] for r in reader.execute("SELECT DISTINCT _source_file FROM lake.logs").fetchall()}
    reader.close()
    assert sources == {f"s3://{BUCKET}/{keys[0]}", f"s3://{BUCKET}/{keys[2]}"}


def test_convert_batch_returns_early_when_no_key_downloaded(tmp_path):
    """Every key failed to download: there is nothing to read, so no
    transaction may be opened — but each key must already carry its own
    terminal-or-retryable status."""
    service_id = "test-batch-edges-nofiles"
    con, cur = _clear_ledger(service_id)
    keys, _ = _write_files(tmp_path, ["gone1", "gone2"])
    _seed(con, cur, service_id, keys)

    fos = MagicMock()
    fos.head_object.side_effect = Exception("An error occurred (404) ... Not Found")
    with _batch_env(service_id, tmp_path, _download_stub({}), fos=fos) as attach:
        summary = convert_batch_objects(service_id, keys, "w1")

    assert summary["dead_letter"] == 2
    assert summary["committed"] == 0
    assert summary["failed"] == 0
    assert {v[0] for v in _rows(con, service_id).values()} == {"dead_letter"}

    reader = _lake_reader(attach)
    assert not reader.execute(
        "SELECT 1 FROM duckdb_tables() WHERE database_name='lake' AND table_name='logs' LIMIT 1"
    ).fetchone()
    reader.close()


# ── per-file bookkeeping across a batch ───────────────────────────────────


def test_convert_batch_widens_a_narrow_table_for_a_newly_enabled_custom_field(tmp_path):
    """Enabling a custom log field changes the read schema. An existing lake
    table missing that column must be ALTERed, or the batch INSERT fails and
    ingest stalls for the whole service the moment a field is added.

    Also pins that the synthetic ``filename`` column read_json_auto adds for
    per-row attribution never becomes a lake column — it is EXCLUDEd from the
    select and skipped by the widening loop."""
    service_id = "test-batch-edges-widen"
    con, cur = _clear_ledger(service_id)
    keys, s3_map = _write_files(tmp_path, ["plain", "custom"], extra={"my_custom_field": "abc"})
    _seed(con, cur, service_id, keys)

    cfg = {
        "service_id": service_id,
        "log_fields": {"custom_fields": [{"name": "my_custom_field", "duckdb_type": "VARCHAR", "enabled": True}]},
    }

    attach = _attacher(str(tmp_path / "cat.ducklake"), str(tmp_path / "lakedata"))
    setup = duckdb.connect()
    attach(setup, None)
    setup.execute("CREATE TABLE lake.logs (timestamp TIMESTAMP, url VARCHAR, _source_file VARCHAR)")
    setup.close()

    with _batch_env(service_id, tmp_path, _download_stub(s3_map), attach=attach, cfg=cfg):
        summary = convert_batch_objects(service_id, keys, "w1")

    assert summary["committed"] == 2, summary
    reader = _lake_reader(attach)
    cols = {r[0] for r in reader.execute("DESCRIBE lake.logs").fetchall()}
    assert "my_custom_field" in cols, "the newly enabled custom field must have widened the table"
    assert "filename" not in cols, "the synthetic attribution column must never reach the lake table"
    values = sorted(r[0] for r in reader.execute("SELECT my_custom_field FROM lake.logs").fetchall())
    reader.close()
    assert values == ["abc", "abc"]


def test_convert_batch_quarantines_per_originating_file(tmp_path):
    """A corrupt line must be reported against the file it came from, not
    the batch. Attributing it to the wrong key sends the operator to the
    wrong raw file."""
    service_id = "test-batch-edges-quarantine"
    con, cur = _clear_ledger(service_id)

    clean = tmp_path / "clean.json"
    clean.write_text(json.dumps({"timestamp": "2026-08-27T16:20:00Z", "url": "/clean"}) + "\n")
    dirty = tmp_path / "dirty.json"
    dirty.write_text(
        json.dumps({"timestamp": "2026-08-27T16:21:00Z", "url": "/good"})
        + "\n"
        + '{"timestamp": "2026-08-27T16:21:01Z", "url": "/trunc"\n'
    )
    clean_key = "raw/2026/08/27/16/20/clean.json.gz"
    dirty_key = "raw/2026/08/27/16/21/dirty.json.gz"
    s3_map = {f"s3://{BUCKET}/{clean_key}": str(clean), f"s3://{BUCKET}/{dirty_key}": str(dirty)}
    _seed(con, cur, service_id, [clean_key, dirty_key])

    fos = MagicMock()
    with _batch_env(service_id, tmp_path, _download_stub(s3_map), fos=fos) as attach:
        summary = convert_batch_objects(service_id, [clean_key, dirty_key], "w1")

    assert summary["committed"] == 2, summary

    # Exactly one file quarantined, and it is the dirty one.
    rows = list_quarantined_files(service_id)
    assert len(rows) == 1
    assert rows[0]["file_name"] == "dirty.json.gz"
    assert rows[0]["fos_key"] == dirty_key
    assert rows[0]["corrupt_rows"] == 1
    assert rows[0]["valid_rows"] == 1

    keys_written = [c.kwargs["Key"] for c in fos.put_object.call_args_list]
    assert keys_written == [
        "errors/2026/08/27/16/21/dirty.json.bad.jsonl",
        "errors/2026/08/27/16/21/dirty.json.bad.jsonl.meta.json",
    ]

    # The NULL-timestamp row is excluded from the lake; the good rows land.
    reader = _lake_reader(attach)
    urls = sorted(r[0] for r in reader.execute("SELECT url FROM lake.logs").fetchall())
    reader.close()
    assert urls == ["/clean", "/good"]
