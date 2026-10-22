"""Batched convert (``convert_batch_objects`` / the ``convert_batch_files``
task): N files must land in ONE DuckLake catalog commit.

Why this matters: the catalog transaction — one Postgres transaction and one
snapshot ROW per commit — is what limits ingest throughput, not celery
dispatch. A live test service showed 27,613 committed files against 27,615
snapshots, i.e. exactly one snapshot per file, growing the snapshot table
linearly with file count forever. These tests assert the collapse itself
(snapshot deltas from ``ducklake_snapshots``), not just the call shape.

Uses a REAL file-backed ``ducklake:`` attach: snapshots and inlining are
DuckLake-specific, and a ``:memory:`` catalog would not survive
``convert_batch_objects`` closing its own connection.

Fixture timestamps are epoch floats because that is what the PRODUCER writes
(``claimed_at = time.time()``, see backend/core/ingest.py).
"""

import json
import time
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.core.ingest import convert_batch_objects
from tests._helpers.ledger import clear_ledger as _clear_ledger

BUCKET = "test-bucket"


def _seed_discovered(con, cur, service_id: str, object_keys: list[str]) -> None:
    for object_key in object_keys:
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
            (service_id, object_key, time.time()),
        )
    con.commit()


def _write_files(tmp_path, names: list[str], lines_per_file: int = 1) -> tuple[list[str], dict[str, str]]:
    """Write one newline-delimited-JSON file per name. Returns the object
    keys and the ``s3://`` -> local-path map the download stub serves."""
    keys: list[str] = []
    s3_to_local: dict[str, str] = {}
    for i, name in enumerate(names):
        local = tmp_path / f"{name}.json"
        local.write_text(
            "".join(
                json.dumps({"timestamp": f"2026-01-01T00:{i:02d}:{j:02d}Z", "url": f"/{name}/{j}"}) + "\n"
                for j in range(lines_per_file)
            )
        )
        key = f"raw/2026/01/01/00/{i:02d}/{name}.json.gz"
        keys.append(key)
        s3_to_local[f"s3://{BUCKET}/{key}"] = str(local)
    return keys, s3_to_local


def _download_stub(s3_to_local: dict[str, str], requested: list[list[str]] | None = None):
    def _download(fos_client, s3_paths, tmpdir):
        if requested is not None:
            requested.append(list(s3_paths))
        # A path absent from the map is a download failure — exactly what
        # _download_chunk_to_local does for a missing object.
        got = {p: s3_to_local[p] for p in s3_paths if p in s3_to_local}
        return got, {v: k for k, v in got.items()}

    return _download


def _attacher(catalog: str, data_path: str, read_only_override: bool = False):
    def _attach(con_arg, src_arg, read_only=False):
        con_arg.execute("INSTALL ducklake; LOAD ducklake;")
        ro = ", READ_ONLY" if (read_only or read_only_override) else ""
        for attempt in range(40):
            try:
                con_arg.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{data_path}'{ro})")
                break
            except duckdb.Error as exc:
                if "already exists" in str(exc).lower():
                    break
                if attempt == 39:
                    raise
                time.sleep(0.05)
        return True

    return _attach


@contextmanager
def _batch_env(service_id: str, tmp_path, download, fos=None, read_only_override: bool = False):
    src = {"service_id": service_id, "name": service_id, "bucket": BUCKET, "prefix": ""}
    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "lakedata")
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.ingest._get_fos_client", return_value=fos if fos is not None else MagicMock()),
        patch("backend.core.ingest._download_chunk_to_local", side_effect=download),
        patch(
            "backend.core.iceberg._ducklake._ducklake_attach",
            side_effect=_attacher(catalog, data_path, read_only_override),
        ),
        patch("backend.core.ingest._configure_fos"),
        patch("backend.core.iceberg._ducklake.ducklake_table_name", return_value="logs"),
    ):
        yield _attacher(catalog, data_path)


def _lake_reader(attach):
    con = duckdb.connect()
    attach(con, None, read_only=False)
    return con


def _snapshots(con) -> int:
    return con.execute("SELECT count(*) FROM ducklake_snapshots('lake')").fetchone()[0]


def test_convert_batch_commits_n_files_in_one_snapshot(tmp_path):
    """THE win: N files -> exactly ONE new catalog snapshot, with all N rows
    present and all N distinct ``_source_file`` values preserved (idempotency
    and the ledger both key off _source_file, so batching must not blur it)."""
    service_id = "test-batch-snapshot-svc"
    con, cur = _clear_ledger(service_id)

    seed_keys, seed_map = _write_files(tmp_path, ["seed"])
    batch_keys, batch_map = _write_files(tmp_path, [f"b{i}" for i in range(8)])
    all_map = {**seed_map, **batch_map}

    _seed_discovered(con, cur, service_id, seed_keys + batch_keys)

    with _batch_env(service_id, tmp_path, _download_stub(all_map)) as attach:
        # First batch creates the lake table — measure from the steady state
        # (DELETE+INSERT into an existing table), which is what prod runs.
        assert convert_batch_objects(service_id, seed_keys, "w-seed")["committed"] == 1

        reader = _lake_reader(attach)
        before = _snapshots(reader)
        rows_before = reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0]
        reader.close()

        summary = convert_batch_objects(service_id, batch_keys, "w-batch")

    assert summary["committed"] == 8, summary
    assert summary["failed"] == 0 and summary["dead_letter"] == 0

    reader = _lake_reader(attach)
    after = _snapshots(reader)
    assert after - before == 1, f"8 files must cost ONE catalog snapshot, not {after - before}"
    assert reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == rows_before + 8
    sources = {r[0] for r in reader.execute("SELECT DISTINCT _source_file FROM lake.logs").fetchall()}
    for key in batch_keys:
        assert f"s3://{BUCKET}/{key}" in sources, f"per-file _source_file lost for {key}"
    reader.close()

    statuses = {
        r[0]
        for r in con.execute(
            "SELECT status FROM ingest_ledger WHERE service_id=? AND object_key IN "
            "(" + ",".join("?" * len(batch_keys)) + ")",
            (service_id, *batch_keys),
        ).fetchall()
    }
    assert statuses == {"committed"}


def test_convert_batch_is_idempotent_on_redelivery(tmp_path):
    """acks_late means a batch can be redelivered. The DELETE-by-_source_file
    inside the same transaction must leave N rows, not 2N."""
    service_id = "test-batch-idempotent-svc"
    con, cur = _clear_ledger(service_id)

    keys, s3_map = _write_files(tmp_path, [f"i{i}" for i in range(5)], lines_per_file=2)
    _seed_discovered(con, cur, service_id, keys)

    with _batch_env(service_id, tmp_path, _download_stub(s3_map)) as attach:
        assert convert_batch_objects(service_id, keys, "w1")["committed"] == 5
        reader = _lake_reader(attach)
        first_rows = reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0]
        reader.close()
        assert first_rows == 10

        # Redelivery: reset the ledger the way a sweeper reclaim would, then
        # run the identical batch again.
        cur.execute(
            "UPDATE ingest_ledger SET status='discovered', claimed_by=NULL, claimed_at=NULL WHERE service_id=?",
            (service_id,),
        )
        con.commit()
        assert convert_batch_objects(service_id, keys, "w2")["committed"] == 5

    reader = _lake_reader(attach)
    assert reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 10, "redelivery must not duplicate rows"
    assert reader.execute("SELECT count(DISTINCT _source_file) FROM lake.logs").fetchone()[0] == 5
    reader.close()


def test_convert_batch_dead_letters_missing_object_and_commits_the_rest(tmp_path):
    """Per-file isolation — the single most important behavioral property:
    one key gone from FOS (genuine 404) must be dead-lettered ALONE while its
    healthy siblings still commit."""
    service_id = "test-batch-deadkey-svc"
    con, cur = _clear_ledger(service_id)

    keys, s3_map = _write_files(tmp_path, [f"d{i}" for i in range(4)])
    dead_key = keys[2]
    del s3_map[f"s3://{BUCKET}/{dead_key}"]  # never downloads
    _seed_discovered(con, cur, service_id, keys)

    fos = MagicMock()
    fos.head_object.side_effect = Exception("An error occurred (404) when calling HeadObject: Not Found")

    with _batch_env(service_id, tmp_path, _download_stub(s3_map), fos=fos) as attach:
        summary = convert_batch_objects(service_id, keys, "w-dead")

    assert summary["committed"] == 3, summary
    assert summary["dead_letter"] == 1
    assert summary["failed"] == 0

    rows = dict(
        con.execute(
            "SELECT object_key, status FROM ingest_ledger WHERE service_id=?",
            (service_id,),
        ).fetchall()
    )
    assert rows[dead_key] == "dead_letter"
    assert all(rows[k] == "committed" for k in keys if k != dead_key)

    reader = _lake_reader(attach)
    sources = {r[0] for r in reader.execute("SELECT DISTINCT _source_file FROM lake.logs").fetchall()}
    assert f"s3://{BUCKET}/{dead_key}" not in sources
    assert len(sources) == 3
    reader.close()


def test_convert_batch_transaction_failure_leaves_every_key_retryable(tmp_path):
    """The transaction is shared, so its failure is the whole batch's: every
    still-claimed key must come back retryable (attempts++/last_error), never
    stranded in 'claimed' where only the reclaim timer could free it."""
    service_id = "test-batch-txnfail-svc"
    con, cur = _clear_ledger(service_id)

    seed_keys, seed_map = _write_files(tmp_path, ["tseed"])
    fail_keys, fail_map = _write_files(tmp_path, [f"t{i}" for i in range(3)])
    _seed_discovered(con, cur, service_id, seed_keys + fail_keys)

    with _batch_env(service_id, tmp_path, _download_stub({**seed_map, **fail_map})):
        assert convert_batch_objects(service_id, seed_keys, "w-seed")["committed"] == 1

    # Re-attach READ_ONLY so the DELETE inside the transaction fails for real
    # instead of being mocked away.
    with _batch_env(service_id, tmp_path, _download_stub(fail_map), read_only_override=True):
        summary = convert_batch_objects(service_id, fail_keys, "w-fail")

    assert summary["committed"] == 0, summary
    assert summary["failed"] == 3

    rows = con.execute(
        "SELECT object_key, status, attempts, last_error FROM ingest_ledger WHERE service_id=? AND status!='committed'",
        (service_id,),
    ).fetchall()
    assert len(rows) == 3
    for row in rows:
        assert row[1] == "discovered", "a failed batch key must be retryable, not stranded in 'claimed'"
        assert row[2] == 1
        assert row[3]


def test_convert_batch_records_per_file_row_counts(tmp_path):
    """ingested_files row counts must be PER FILE. Recording the batch total
    against each key would break Usage Log / log-line-accounting
    reconciliation."""
    service_id = "test-batch-rowcount-svc"
    con, cur = _clear_ledger(service_id)
    con.execute("DELETE FROM ingested_files WHERE source_name=?", (service_id,))
    con.commit()

    keys: list[str] = []
    s3_map: dict[str, str] = {}
    expected: dict[str, int] = {}
    for i, n_lines in enumerate((1, 3, 7)):
        local = tmp_path / f"rc{i}.json"
        local.write_text(
            "".join(json.dumps({"timestamp": f"2026-01-01T00:0{i}:{j:02d}Z"}) + "\n" for j in range(n_lines))
        )
        key = f"raw/2026/01/01/00/0{i}/rc{i}.json.gz"
        keys.append(key)
        s3_map[f"s3://{BUCKET}/{key}"] = str(local)
        expected[key] = n_lines
    _seed_discovered(con, cur, service_id, keys)

    with _batch_env(service_id, tmp_path, _download_stub(s3_map)):
        assert convert_batch_objects(service_id, keys, "w-rc")["committed"] == 3

    recorded = dict(
        con.execute(
            "SELECT file_name, row_count FROM ingested_files WHERE source_name=?",
            (service_id,),
        ).fetchall()
    )
    assert recorded == expected, "row counts must be per file, not the batch total"


def test_convert_batch_proceeds_with_only_the_keys_it_won(tmp_path):
    """Partial claim: keys already 'committed' or 'claimed' by another worker
    are silently skipped (the batch's 'not_claimed'), and are never even
    downloaded — the rest of the batch proceeds."""
    service_id = "test-batch-partial-svc"
    con, cur = _clear_ledger(service_id)

    keys, s3_map = _write_files(tmp_path, ["mine", "already", "theirs"])
    mine, already, theirs = keys
    _seed_discovered(con, cur, service_id, [mine])
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at, committed_at) "
        "VALUES (?, ?, 'committed', ?, ?)",
        (service_id, already, time.time(), time.time()),
    )
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at) "
        "VALUES (?, ?, 'claimed', 'other-worker', ?, ?)",
        (service_id, theirs, time.time(), time.time()),
    )
    con.commit()

    requested: list[list[str]] = []
    with _batch_env(service_id, tmp_path, _download_stub(s3_map, requested)) as attach:
        summary = convert_batch_objects(service_id, keys, "w-partial")

    assert summary["claimed"] == 1
    assert summary["not_claimed"] == 2
    assert summary["committed"] == 1

    assert requested == [[f"s3://{BUCKET}/{mine}"]], "a key we did not win must not even be downloaded"

    rows = dict(
        con.execute("SELECT object_key, status FROM ingest_ledger WHERE service_id=?", (service_id,)).fetchall()
    )
    assert rows == {mine: "committed", already: "committed", theirs: "claimed"}

    reader = _lake_reader(attach)
    sources = {r[0] for r in reader.execute("SELECT DISTINCT _source_file FROM lake.logs").fetchall()}
    assert sources == {f"s3://{BUCKET}/{mine}"}
    reader.close()


def test_convert_batch_downloads_outside_admission_lock(tmp_path):
    """The per-service DuckLake write-admission lock must NOT be held during
    the FOS download. Holding it across up to LEDGER_CONVERT_BATCH_SIZE (50)
    GETs made the median batch lock-hold 38.5s (max 68.9s) — exceeding the 30s
    admission timeout — so concurrent batches timed out before the holder
    released, cascading (measured 2026-10-02: 319 advisory-lock timeouts in
    3min, 228 request + 95 rum) and starving lower-volume RUM converts to a
    0-beacon recent window under verify-phase load. The download touches no
    DuckLake catalog state, so the lock must wrap only attach + INSERT."""
    service_id = "test-batch-dl-outside-lock-svc"
    con, cur = _clear_ledger(service_id)
    keys, s3_map = _write_files(tmp_path, [f"x{i}" for i in range(4)])
    _seed_discovered(con, cur, service_id, keys)

    state = {"held": False, "held_during_download": None}

    @contextmanager
    def _tracking_admission(sid, *args, **kwargs):
        state["held"] = True
        try:
            yield
        finally:
            state["held"] = False

    def _download(fos_client, s3_paths, tmpdir):
        state["held_during_download"] = state["held"]
        got = {p: s3_map[p] for p in s3_paths if p in s3_map}
        return got, {v: k for k, v in got.items()}

    with _batch_env(service_id, tmp_path, _download):
        with patch("backend.core.ducklake_admission.ducklake_write_admission", _tracking_admission):
            summary = convert_batch_objects(service_id, keys, "w-dl")

    assert summary["committed"] == 4, summary
    assert state["held_during_download"] is False, (
        "FOS download must happen OUTSIDE the admission lock — holding the "
        "per-service write lock across network I/O starves concurrent converts"
    )


def test_convert_batch_task_routes_to_ingest_queue():
    """A task with no consumer is a shipped no-op (this branch already had
    one). Pin that convert_batch_files is registered and lands on q.ingest
    under the existing wildcard route, which the workers' -Q covers."""
    from backend.celery_app import app

    name = "backend.core.ingest.convert_batch_files"
    assert name in app.tasks
    assert app.amqp.router.route({}, name)["queue"].name == "q.ingest"


@pytest.mark.parametrize("missing_count", [0, 2])
def test_convert_batch_coalesces_cron_run_sse_publish_to_once_per_originating_run(tmp_path, missing_count):
    """Regression test for the High-Scale /api/cron-runs refetch + ERR_ABORTED storm:
    when a batch of N files shares an originating ``log_discovery`` run_id,
    ``convert_batch_objects`` must aggregate all N object outcomes into
    ``cron_runs`` and publish ``cron_run_changed`` at most ONCE per originating
    run — never once per file (which emitted up to 50 SSE events in ~1.3s)."""
    from backend.core import metadata as metadata_db

    service_id = "test-batch-sse-coalesce-svc"
    con, cur = _clear_ledger(service_id)
    con.execute("DELETE FROM cron_runs WHERE service_id=?", (service_id,))
    con.commit()

    run_id = metadata_db.start_cron_run(service_id, "log_discovery")
    metadata_db.log_cron_run(service_id, "log_discovery", duration_s=0.2, status="success", run_id=run_id)

    keys, s3_map = _write_files(tmp_path, [f"s{i}" for i in range(8)], lines_per_file=2)
    for object_key in keys:
        cur.execute(
            "INSERT INTO ingest_ledger "
            "(service_id, object_key, status, discovered_at, originating_task, originating_run_id) "
            "VALUES (?, ?, 'discovered', ?, 'log_discovery', ?)",
            (service_id, object_key, time.time(), run_id),
        )
    con.commit()

    for object_key in keys[:missing_count]:
        del s3_map[f"s3://{BUCKET}/{object_key}"]
    fos = MagicMock()
    fos.head_object.side_effect = Exception("An error occurred (404) when calling HeadObject: Not Found")
    with (
        _batch_env(service_id, tmp_path, _download_stub(s3_map), fos=fos),
        patch("backend.core.metadata.cron_log._publish_cron_run_change") as mock_publish,
    ):
        summary = convert_batch_objects(service_id, keys, "w-sse")

    expected_committed = 8 - missing_count
    assert summary["committed"] == expected_committed, summary
    assert summary["dead_letter"] == missing_count, summary
    row = con.execute(
        "SELECT status, rows_ingested, outcome_counters FROM cron_runs WHERE id=? AND service_id=?",
        (run_id, service_id),
    ).fetchone()
    assert row[0] == ("error" if missing_count else "success")
    assert row[1] == expected_committed * 2
    counters = json.loads(row[2])
    assert counters["objects_failed"] == missing_count
    assert counters["valid_records"] == expected_committed * 2
    if missing_count:
        mock_publish.assert_called_once_with(service_id, run_id, "log_discovery", "error")
    else:
        mock_publish.assert_not_called()
    with (
        _batch_env(service_id, tmp_path, _download_stub(s3_map), fos=fos),
        patch("backend.core.metadata.cron_log._publish_cron_run_change") as replay_publish,
    ):
        replay = convert_batch_objects(service_id, keys, "w-sse-replay")
    assert replay["committed"] == 0
    replay_publish.assert_not_called()
    assert (
        con.execute("SELECT rows_ingested FROM cron_runs WHERE id=? AND service_id=?", (run_id, service_id)).fetchone()[
            0
        ]
        == expected_committed * 2
    )
