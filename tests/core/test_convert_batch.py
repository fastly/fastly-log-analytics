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

from __future__ import annotations

import inspect
import json
import os
import string
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from functools import partial
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from backend.core.metadata.quarantine import list_quarantine_evidence
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, consumes, initialize, invariant, rule
from tests._helpers.ledger import (
    DuckLakeLedgerEnv,
    clear_ledger,
    ledger_row,
    seed_discovered,
)
from tests._helpers.ledger import (
    attacher as _attacher,
)
from tests._helpers.ledger import (
    clear_ledger as _clear_ledger,
)
from tests._helpers.ledger import (
    download_stub as _download_stub,
)
from tests._helpers.ledger import (
    lake_reader as _lake_reader,
)
from tests._helpers.ledger import (
    seed_discovered as _seed,
)
from tests._helpers.ledger import (
    seed_discovered as _seed_discovered,
)
from tests._helpers.sources import make_source

from backend.core import duckdb as _ddb
from backend.core import ingest
from backend.core import metadata as metadata_db
from backend.core.ingest import (
    _compute_incremental_start_after,
    _delete_objects_robust,
    _delete_objects_robust_with_failures,
    _parse_fastly_filename_dt,
    _quarantine_convert_corrupt_lines,
    convert_batch_objects,
    discover_prefix,
    get_catalog_field_ids,
    get_ingest_columns_sql,
    get_ingest_type_hints,
)
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.core.metadata import base as metadata_base
from backend.core.metadata.base import get_con
from tests.core.test_ingest_discovery import (  # reuse fixture helpers
    _install_mock_fos,
    _S3RewritingConn,
    _seed_local,
    ingest_local_env,  # noqa: F401 — pytest fixture re-export
)

# ── Merged from tests/core/test_convert_batch.py ──
BUCKET = "test-bucket"


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


def _batch_env(service_id: str, tmp_path, download, fos=None, read_only_override: bool = False):
    return DuckLakeLedgerEnv(
        service_id,
        tmp_path,
        download=download,
        fos=fos,
        bucket=BUCKET,
        read_only_override=read_only_override,
    )


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
        with patch("backend.core.iceberg._ducklake.ducklake_write_admission", _tracking_admission):
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


# ── Merged from tests/core/test_convert_batch_edges.py ──
def _rows(con, service_id: str) -> dict[str, tuple]:
    return {
        r["object_key"]: (r["status"], r["attempts"], r["last_error"], r["committed_at"])
        for r in con.execute(
            "SELECT object_key, status, attempts, last_error, committed_at FROM ingest_ledger WHERE service_id=?",
            (service_id,),
        ).fetchall()
    }


def _write_files_convert_batch_edges(
    tmp_path, names: list[str], extra: dict | None = None
) -> tuple[list[str], dict[str, str]]:
    keys: list[str] = []
    s3_to_local: dict[str, str] = {}
    for i, name in enumerate(names):
        record = {"timestamp": f"2026-08-27T16:{i:02d}:00Z", "url": f"/{name}"}
        record.update(extra or {})
        local = tmp_path / f"{name}.json"
        local.write_text(json.dumps(record) + "\n")
        key = f"raw/request/year=2026/month=08/day=27/hour=16/minute={i:02d}/{name}.json.gz"
        keys.append(key)
        s3_to_local[f"s3://{BUCKET}/{key}"] = str(local)
    return keys, s3_to_local


def _batch_env_convert_batch_edges(service_id: str, tmp_path, download, fos=None, attach=None, cfg=None):
    return DuckLakeLedgerEnv(
        service_id,
        tmp_path,
        download=download,
        fos=fos,
        attach=attach,
        cfg=cfg,
        bucket=BUCKET,
    )


def test_convert_batch_with_an_empty_key_list_does_nothing():
    """The dispatcher slices keys into batches; an empty slice must not open
    a DuckLake connection or a catalog transaction."""
    service_id = "test-batch-edges-empty"
    clear_ledger(service_id, quarantine=True)

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
    con, cur = clear_ledger(service_id, quarantine=True)
    keys, _ = _write_files_convert_batch_edges(tmp_path, ["nc1", "nc2"])
    _seed(con, cur, service_id, keys, status="committed")

    with patch("backend.core.duckdb.get_source_for_service") as mock_src:
        summary = convert_batch_objects(service_id, keys, "w1")

    mock_src.assert_not_called()
    assert summary["requested"] == 2
    assert summary["claimed"] == 0
    assert summary["not_claimed"] == 2
    assert summary["committed"] == 0
    assert {v[0] for v in _rows(con, service_id).values()} == {"committed"}


def test_convert_batch_without_a_registered_source_requeues_every_claimed_key(tmp_path):
    """The keys are already ``claimed`` at this point — returning without
    resetting them would strand all of them until the reclaim timeout."""
    service_id = "test-batch-edges-nosrc"
    con, cur = clear_ledger(service_id, quarantine=True)
    keys, _ = _write_files_convert_batch_edges(tmp_path, ["ns1", "ns2", "ns3"])
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
    con, cur = clear_ledger(service_id, quarantine=True)
    keys, s3_map = _write_files_convert_batch_edges(tmp_path, ["at1", "at2"])
    _seed(con, cur, service_id, keys)

    with _batch_env_convert_batch_edges(service_id, tmp_path, _download_stub(s3_map), attach=lambda *a, **kw: False):
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
    con, cur = clear_ledger(service_id, quarantine=True)
    keys, s3_map = _write_files_convert_batch_edges(tmp_path, ["bk1", "bk2"])
    _seed(con, cur, service_id, keys)

    with _batch_env_convert_batch_edges(service_id, tmp_path, _download_stub(s3_map)) as attach:
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


def test_convert_batch_transient_download_failure_does_not_strand_siblings(tmp_path):
    """One key whose object still HEADs fine gets requeued; the rest of the
    batch commits in the same run. Failing the whole batch for one blip
    would re-download the siblings on every retry."""
    service_id = "test-batch-edges-transient"
    con, cur = clear_ledger(service_id, quarantine=True)
    keys, s3_map = _write_files_convert_batch_edges(tmp_path, ["ok1", "blip", "ok2"])
    _seed(con, cur, service_id, keys)

    blip_key = keys[1]
    del s3_map[f"s3://{BUCKET}/{blip_key}"]

    fos = MagicMock()
    fos.head_object.return_value = {"ContentLength": 5}
    with _batch_env_convert_batch_edges(service_id, tmp_path, _download_stub(s3_map), fos=fos) as attach:
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
    con, cur = clear_ledger(service_id, quarantine=True)
    keys, _ = _write_files_convert_batch_edges(tmp_path, ["gone1", "gone2"])
    _seed(con, cur, service_id, keys)

    fos = MagicMock()
    fos.head_object.side_effect = Exception("An error occurred (404) ... Not Found")
    with _batch_env_convert_batch_edges(service_id, tmp_path, _download_stub({}), fos=fos) as attach:
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


def test_convert_batch_widens_a_narrow_table_for_a_newly_enabled_custom_field(tmp_path):
    """Enabling a custom log field changes the read schema. An existing lake
    table missing that column must be ALTERed, or the batch INSERT fails and
    ingest stalls for the whole service the moment a field is added.

    Also pins that the synthetic ``filename`` column read_json_auto adds for
    per-row attribution never becomes a lake column — it is EXCLUDEd from the
    select and skipped by the widening loop."""
    service_id = "test-batch-edges-widen"
    con, cur = clear_ledger(service_id, quarantine=True)
    keys, s3_map = _write_files_convert_batch_edges(tmp_path, ["plain", "custom"], extra={"my_custom_field": "abc"})
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

    with _batch_env_convert_batch_edges(service_id, tmp_path, _download_stub(s3_map), attach=attach, cfg=cfg):
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
    con, cur = clear_ledger(service_id, quarantine=True)

    clean = tmp_path / "clean.json"
    clean.write_text(json.dumps({"timestamp": "2026-08-27T16:20:00Z", "url": "/clean"}) + "\n")
    dirty = tmp_path / "dirty.json"
    dirty.write_text(
        json.dumps({"timestamp": "2026-08-27T16:21:00Z", "url": "/good"})
        + "\n"
        + '{"timestamp": "2026-08-27T16:21:01Z", "url": "/trunc"\n'
    )
    clean_key = "raw/request/year=2026/month=08/day=27/hour=16/minute=20/clean.json.gz"
    dirty_key = "raw/request/year=2026/month=08/day=27/hour=16/minute=21/dirty.json.gz"
    s3_map = {f"s3://{BUCKET}/{clean_key}": str(clean), f"s3://{BUCKET}/{dirty_key}": str(dirty)}
    _seed(con, cur, service_id, [clean_key, dirty_key])

    fos = MagicMock()
    with _batch_env_convert_batch_edges(service_id, tmp_path, _download_stub(s3_map), fos=fos) as attach:
        summary = convert_batch_objects(service_id, [clean_key, dirty_key], "w1")

    assert summary["committed"] == 2, summary

    # The exact malformed line is stored locally against its originating object.
    rows = list_quarantine_evidence(service_id)
    assert len(rows) == 1
    item = rows[0]
    assert item["source_type"] == "request"
    assert item["original_key"] == dirty_key
    assert item["line_ordinal"] == 2
    assert item["error_category"] == "invalid_json"
    evidence_path = tmp_path / "data" / "services" / service_id / "quarantine" / item["file_path"]
    assert evidence_path.read_bytes() == b'{"timestamp": "2026-08-27T16:21:01Z", "url": "/trunc"\n'
    fos.put_object.assert_not_called()

    # The NULL-timestamp row is excluded from the lake; the good rows land.
    reader = _lake_reader(attach)
    urls = sorted(r[0] for r in reader.execute("SELECT url FROM lake.logs").fetchall())
    reader.close()
    assert urls == ["/clean", "/good"]


# ── Merged from tests/core/test_convert_object_edges.py ──
SERVICE_ID = "test-convert-object-edges"

SRC = {"service_id": SERVICE_ID, "name": SERVICE_ID, "bucket": BUCKET, "prefix": ""}


def _seed_discovered_edge(con, object_key: str) -> None:
    seed_discovered(con, con.cursor(), SERVICE_ID, [object_key])


def _ledger_row(con, object_key: str):
    return ledger_row(con, SERVICE_ID, object_key)


@contextmanager
def _convert_env(tmp_path, download, fos=None, attach=None):
    with DuckLakeLedgerEnv(
        SERVICE_ID,
        tmp_path,
        download,
        bucket=BUCKET,
        fos=fos,
        attach=attach,
        table_name="logs",
    ) as real_attach:
        yield real_attach


def _write_ndjson(path, records: list[dict]) -> str:
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    return str(path)


def test_convert_object_with_no_registered_source_stays_retryable():
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=00/nosrc.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered_edge(con, object_key)

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
        assert summary["failed"] == 1

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "no source registered" in row["last_error"]
    assert row["committed_at"] is None


def test_convert_object_with_failed_lake_attach_stays_retryable(tmp_path):
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=01/noattach.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered_edge(con, object_key)

    local = _write_ndjson(tmp_path / "noattach.json", [{"timestamp": "2026-08-27T15:01:00Z", "url": "/a"}])
    with _convert_env(
        tmp_path,
        _download_stub({f"s3://{BUCKET}/{object_key}": local}),
        attach=lambda *a, **kw: False,
    ):
        summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
        assert summary["failed"] == 1

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "attach failed" in row["last_error"]


def test_convert_object_dead_letters_a_key_whose_object_is_gone(tmp_path):
    """A resurrected ledger row for a file the old pipeline already ingested
    and deleted is terminal — otherwise the sweeper re-GETs it forever."""
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=02/vanished.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered_edge(con, object_key)

    fos = MagicMock()
    fos.head_object.side_effect = Exception("An error occurred (NoSuchKey) when calling the HeadObject operation")
    with _convert_env(tmp_path, _download_stub({}), fos=fos):
        summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
        assert summary["dead_letter"] == 1

    row = _ledger_row(con, object_key)
    assert row["status"] == "dead_letter"
    assert row["attempts"] == 0  # a decision, not a failed attempt
    assert "missing from FOS" in row["last_error"]
    fos.head_object.assert_called_once_with(Bucket=BUCKET, Key=object_key)


def test_convert_object_treats_download_failure_on_a_live_object_as_transient(tmp_path):
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=03/blip.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered_edge(con, object_key)

    fos = MagicMock()
    fos.head_object.return_value = {"ContentLength": 42}
    with _convert_env(tmp_path, _download_stub({}), fos=fos):
        summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
        assert summary["failed"] == 1

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "transient" in row["last_error"]


def test_convert_object_widens_the_lake_table_for_a_new_custom_field(tmp_path):
    """Custom log fields appear over time. An existing table missing a
    column must be ALTERed rather than the insert failing — a failed widen
    would stall ingest for the whole service the moment a field is added."""
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=04/widen.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered_edge(con, object_key)

    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "lakedata")
    attach = _attacher(catalog, data_path)

    setup = duckdb.connect()
    attach(setup, None)
    setup.execute("CREATE TABLE lake.logs (timestamp TIMESTAMP, url VARCHAR, _source_file VARCHAR)")
    setup.execute("INSERT INTO lake.logs VALUES ('2026-08-01T00:00:00', '/pre-existing', 's3://other/old.gz')")
    setup.close()

    local = _write_ndjson(
        tmp_path / "widen.json",
        [{"timestamp": "2026-08-27T15:04:00Z", "url": "/new", "status": 200}],
    )
    with _convert_env(
        tmp_path,
        _download_stub({f"s3://{BUCKET}/{object_key}": local}),
        attach=attach,
    ):
        summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
        assert summary["committed"] == 1

    assert _ledger_row(con, object_key)["status"] == "committed"
    counters = json.loads(
        con.execute(
            "SELECT outcome_counters FROM ingest_ledger WHERE service_id=? AND object_key=?",
            (SERVICE_ID, object_key),
        ).fetchone()[0]
    )
    assert counters["valid_records"] == 1
    assert counters["objects_processed"] == 1
    assert counters["objects_successful"] == 1
    assert counters["objects_failed"] == 0
    reader = _lake_reader(attach, read_only=True)
    cols = {r[0] for r in reader.execute("DESCRIBE lake.logs").fetchall()}
    assert "status" in cols, "the new field must have been added to the table"
    rows = reader.execute("SELECT url, _source_file FROM lake.logs ORDER BY url").fetchall()
    reader.close()
    # The pre-existing row is untouched; the new one is attributed to this key.
    assert rows == [("/new", f"s3://{BUCKET}/{object_key}"), ("/pre-existing", "s3://other/old.gz")]


def test_convert_object_rolls_back_and_stays_retryable_when_the_insert_cannot_land(tmp_path):
    """A lake table whose column type the incoming data cannot cast to must
    abort inside the transaction: no partial rows, no committed ledger
    row."""
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=05/drift.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered_edge(con, object_key)

    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "lakedata")
    attach = _attacher(catalog, data_path)

    setup = duckdb.connect()
    attach(setup, None)
    # url typed INTEGER: '/a/b' cannot cast, so the INSERT raises.
    setup.execute("CREATE TABLE lake.logs (timestamp TIMESTAMP, url INTEGER, _source_file VARCHAR)")
    setup.close()

    local = _write_ndjson(tmp_path / "drift.json", [{"timestamp": "2026-08-27T15:05:00Z", "url": "/a/b"}])
    with _convert_env(
        tmp_path,
        _download_stub({f"s3://{BUCKET}/{object_key}": local}),
        attach=attach,
    ):
        summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
        assert summary["failed"] == 1

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["committed_at"] is None
    assert row["attempts"] == 1

    reader = _lake_reader(attach, read_only=True)
    assert reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 0
    reader.close()


def test_convert_object_commits_even_when_the_quarantine_check_blows_up(tmp_path):
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=06/qfail.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered_edge(con, object_key)

    local_path = tmp_path / "qfail.json"
    local = _write_ndjson(local_path, [{"timestamp": "2026-08-27T15:06:00Z", "url": "/a"}])
    with _convert_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": local})) as attach:
        with patch(
            "backend.core.quarantine.capture_evidence",
            return_value={"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1},
        ):
            local_path.write_text('{"timestamp":"2026-08-27T15:06:00Z","url":"/a"}\n{"url":"/bad"}\n')
            summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
            assert summary["committed"] == 1

    assert _ledger_row(con, object_key)["status"] == "committed"
    counters = json.loads(
        con.execute(
            "SELECT outcome_counters FROM ingest_ledger WHERE service_id=? AND object_key=?",
            (SERVICE_ID, object_key),
        ).fetchone()[0]
    )
    assert counters["valid_records"] == 1
    assert counters["malformed_records"] == 1
    assert counters["quarantine_capture_failures"] == 1
    assert counters["objects_failed"] == 1
    reader = _lake_reader(attach, read_only=True)
    assert reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 1
    reader.close()


def test_convert_object_commits_even_when_ingested_files_bookkeeping_fails(tmp_path):
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=07/bkfail.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered_edge(con, object_key)

    local = _write_ndjson(tmp_path / "bkfail.json", [{"timestamp": "2026-08-27T15:07:00Z", "url": "/a"}])
    with _convert_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": local})) as attach:
        with patch(
            "backend.core.ingest.metadata_db.insert_ingested_files",
            side_effect=Exception("metadata db locked"),
        ):
            summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
            assert summary["committed"] == 1

    assert _ledger_row(con, object_key)["status"] == "committed"
    reader = _lake_reader(attach, read_only=True)
    assert reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 1
    reader.close()


def _empty_list_fos(*args, **kwargs):
    yield {"type": "status", "message": "Discovering"}
    return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}


def _null_list_fos(*args, **kwargs):
    yield {"type": "status", "message": "Discovering"}
    return None


def test_discover_prefix_is_a_noop_without_config_or_source():
    con, _ = clear_ledger(SERVICE_ID)

    with patch("backend.config.load_config", return_value=None):
        with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos) as mock_list:
            assert discover_prefix(SERVICE_ID) == 0
            mock_list.assert_not_called()

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value=None):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos) as mock_list:
                assert discover_prefix(SERVICE_ID) == 0
                mock_list.assert_not_called()

    assert con.execute("SELECT count(*) FROM ingest_ledger WHERE service_id=?", (SERVICE_ID,)).fetchone()[0] == 0


def test_discover_prefix_returns_zero_when_the_list_yields_no_result():
    """An aborted LIST is not an empty bucket — reporting it as a successful
    scan of zero files would let the caller advance its watermark."""
    clear_ledger(SERVICE_ID)
    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value=SRC):
            with patch("backend.core.ingest.list_fos_files", side_effect=_null_list_fos):
                with patch("backend.core.ingest.convert_batch_files.delay") as mock_delay:
                    assert discover_prefix(SERVICE_ID) == 0
                    mock_delay.assert_not_called()


def test_discover_prefix_omits_prefix_subpath_when_not_given():
    """Unlike the RUM twin, discover_prefix must NOT pass a prefix_subpath
    of its own — list_fos_files' default (the regular-log root) is the
    intended scan."""
    clear_ledger(SERVICE_ID)
    captured: dict = {}

    def _capture(*args, **kwargs):
        captured.update(kwargs)
        yield {"type": "status", "message": "Discovering"}
        return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value=SRC):
            with patch("backend.core.ingest.list_fos_files", side_effect=_capture):
                assert discover_prefix(SERVICE_ID) == 0

    assert "prefix_subpath" not in captured
    assert captured["start_time"] is None


def test_discover_prefix_persists_the_originating_cron_run():
    clear_ledger(SERVICE_ID)
    object_key = "raw/request/year=2026/month=08/day=27/hour=13/minute=05/part.json.gz"

    def list_one_file(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        return {
            "new_files": [f"s3://{BUCKET}/{object_key}"],
            "file_sizes": {f"s3://{BUCKET}/{object_key}": 128},
            "skipped_already": 0,
            "stranded_already": [],
        }

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value=SRC):
            with patch("backend.core.ingest.list_fos_files", side_effect=list_one_file):
                with patch("backend.core.ingest.convert_batch_files.delay") as dispatch:
                    assert discover_prefix(SERVICE_ID, run_id=812) == 1
                    dispatch.assert_called_once_with(SERVICE_ID, [object_key])
                    assert (
                        discover_prefix(
                            SERVICE_ID,
                            run_id=913,
                            originating_task="full_sync",
                        )
                        == 0
                    )
                    dispatch.assert_called_once_with(SERVICE_ID, [object_key])

    row = (
        get_con(SERVICE_ID)
        .execute(
            "SELECT originating_task, originating_run_id FROM ingest_ledger WHERE service_id=? AND object_key=?",
            (SERVICE_ID, object_key),
        )
        .fetchone()
    )
    assert row["originating_task"] == "log_discovery"
    assert row["originating_run_id"] == 812


def _read_expr(local_file: str) -> str:
    return (
        f"read_json_auto('{local_file}', format='newline_delimited', "
        f"records='auto', columns={get_ingest_columns_sql(None)}, ignore_errors=true)"
    )


def _clear_quarantine(service_id: str):
    con = get_con(service_id)
    con.execute("DELETE FROM quarantined_files")
    con.commit()


def test_quarantine_convert_corrupt_lines_records_a_truncated_line(tmp_path):
    """The reference case: a truncated line becomes an all-NULL row under
    ignore_errors=true, is excluded from the lake insert by the caller, and
    must still be reported so the row-count shortfall is explainable."""
    _clear_quarantine(SERVICE_ID)
    local = tmp_path / "mixed.json"
    local.write_text(
        json.dumps({"timestamp": "2026-08-27T15:08:00Z", "url": "/ok"})
        + "\n"
        + '{"timestamp": "2026-08-27T15:08:01Z", "url": "/trunc"\n'
    )
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=08/mixed.json.gz"
    fos = MagicMock()
    con = duckdb.connect()

    with patch("backend.core.quarantine.capture_evidence") as capture:
        capture.return_value = {"id": 1, "cap_evictions": 0, "quarantine_capture_failures": 0}
        result = _quarantine_convert_corrupt_lines(con, fos, SRC, _read_expr(str(local)), str(local), object_key)
    con.close()

    fos.put_object.assert_not_called()
    assert result == {
        "valid_records": 1,
        "malformed_records": 1,
        "cap_evictions": 0,
        "quarantine_capture_failures": 0,
    }
    capture.assert_called_once()
    assert capture.call_args.args == (
        SERVICE_ID,
        "request",
        object_key,
        b'{"timestamp": "2026-08-27T15:08:01Z", "url": "/trunc"\n',
    )


def test_request_worker_quarantine_captures_exact_line_bytes_without_fos_upload(tmp_path, monkeypatch):
    from backend.core import quarantine

    local = tmp_path / "mixed.json"
    valid_line = b'{"timestamp":"2026-08-27T15:08:00Z","url":"/ok"}\n'
    bad_line = b'{"timestamp":"2026-08-27T15:08:01Z","url":"/bad"\n'
    local.write_bytes(valid_line + bad_line)
    calls = []

    def capture(*args, **kwargs):
        calls.append((args, kwargs))
        return {"id": 12, "cap_evictions": 1, "quarantine_capture_failures": 0}

    monkeypatch.setattr(quarantine, "capture_evidence", capture)
    con = duckdb.connect()
    try:
        result = _quarantine_convert_corrupt_lines(
            con,
            MagicMock(),
            SRC,
            _read_expr(str(local)),
            str(local),
            "raw/request/mixed.json.gz",
        )
    finally:
        con.close()

    assert result == {
        "valid_records": 1,
        "malformed_records": 1,
        "cap_evictions": 1,
        "quarantine_capture_failures": 0,
    }
    assert len(calls) == 1
    assert calls[0][0][0:3] == (SERVICE_ID, "request", "raw/request/mixed.json.gz")
    assert calls[0][0][3] == bad_line
    assert calls[0][1]["line_ordinal"] == 2
    assert calls[0][1]["byte_offset"] == len(valid_line)


def test_quarantine_convert_corrupt_lines_does_nothing_for_a_clean_file(tmp_path):
    _clear_quarantine(SERVICE_ID)
    local = tmp_path / "clean.json"
    local.write_text(json.dumps({"timestamp": "2026-08-27T15:09:00Z", "url": "/ok"}) + "\n")
    fos = MagicMock()
    con = duckdb.connect()

    result = _quarantine_convert_corrupt_lines(
        con,
        fos,
        SRC,
        _read_expr(str(local)),
        str(local),
        "raw/request/year=2026/month=08/day=27/hour=15/minute=09/clean.json.gz",
    )
    con.close()

    fos.put_object.assert_not_called()
    assert result["malformed_records"] == 0


def test_quarantine_convert_corrupt_lines_skips_upload_when_the_detectors_disagree(tmp_path):
    """The row-level NULL count and the raw-line scan are independent
    detectors, kept independent on purpose. A line whose timestamp is
    present-but-uncastable trips the first and not the second: there is
    nothing concrete to attach to a sidecar, so nothing may be uploaded or
    registered (an empty .bad.jsonl would be worse than a log line)."""
    _clear_quarantine(SERVICE_ID)
    local = tmp_path / "disagree.json"
    local.write_text(json.dumps({"timestamp": "definitely-not-a-timestamp", "url": "/x"}) + "\n")
    fos = MagicMock()
    con = duckdb.connect()

    valid, corrupt = con.execute(
        f"SELECT count(*) FILTER (timestamp IS NOT NULL), count(*) FILTER (timestamp IS NULL) "
        f"FROM {_read_expr(str(local))}"
    ).fetchone()
    assert (valid, corrupt) == (0, 1), "precondition: the row-level detector must see this as corrupt"

    result = _quarantine_convert_corrupt_lines(
        con,
        fos,
        SRC,
        _read_expr(str(local)),
        str(local),
        "raw/request/year=2026/month=08/day=27/hour=15/minute=10/disagree.json.gz",
    )
    con.close()

    fos.put_object.assert_not_called()
    assert result["quarantine_capture_failures"] == 1


def test_quarantine_convert_corrupt_lines_skips_a_key_outside_the_raw_prefix(tmp_path):
    """The errors key is built by slicing the raw prefix off the object key —
    a key that does not start with it must be skipped, not mangled into an
    unrelated destination."""
    _clear_quarantine(SERVICE_ID)
    local = tmp_path / "offprefix.json"
    local.write_text('{"timestamp": "2026-08-27T15:11:00Z", "url": "/trunc"\n')
    fos = MagicMock()
    con = duckdb.connect()

    result = _quarantine_convert_corrupt_lines(
        con, fos, SRC, _read_expr(str(local)), str(local), "somewhere/else/offprefix.json.gz"
    )
    con.close()

    fos.put_object.assert_not_called()
    assert result["quarantine_capture_failures"] == 1


def test_quarantine_convert_corrupt_gzip_captures_the_original_container_bytes(tmp_path):
    payload = b"not a gzip container\x00\xff"
    local = tmp_path / "broken.json.gz"
    local.write_bytes(payload)
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = (0, 1)
    key = "raw/request/year=2026/month=08/day=27/hour=15/minute=12/broken.json.gz"

    with patch("backend.core.quarantine.capture_evidence") as capture:
        capture.return_value = {"cap_evictions": 1, "quarantine_capture_failures": 0}
        result = _quarantine_convert_corrupt_lines(con, MagicMock(), SRC, "ignored", str(local), key)

    assert result == {
        "valid_records": 0,
        "malformed_records": 0,
        "corrupt_containers": 1,
        "cap_evictions": 1,
        "quarantine_capture_failures": 0,
    }
    assert capture.call_args.args == (SERVICE_ID, "request", key, payload)
    assert capture.call_args.kwargs["error_category"] == "corrupt_container"


def test_quarantine_convert_corrupt_gzip_reports_when_container_cannot_be_read(tmp_path, monkeypatch):
    local = tmp_path / "unreadable.json.gz"
    local.write_bytes(b"not gzip")
    real_open = open

    def fail_open(path, *args, **kwargs):
        if str(path) == str(local):
            raise OSError("local cache unavailable")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr("builtins.open", fail_open)
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = (0, 1)
    result = _quarantine_convert_corrupt_lines(
        con,
        MagicMock(),
        SRC,
        "ignored",
        str(local),
        "raw/request/year=2026/month=08/day=27/hour=15/minute=13/unreadable.json.gz",
    )

    assert result == {
        "valid_records": 0,
        "malformed_records": 0,
        "corrupt_containers": 1,
        "cap_evictions": 0,
        "quarantine_capture_failures": 1,
    }


# ── Merged from tests/core/test_ingest.py ──


def test_ingest_type_hints():
    hints = get_ingest_type_hints()
    assert len(hints) == len([f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None])
    assert hints["timestamp"] == "TIMESTAMPTZ"
    ip_field = next(f for f in LOG_FIELD_CATALOG if f["id"] == "ip")
    assert hints["ip"] == ip_field["duckdb_type"]


def test_ingest_type_hints_with_custom_fields():
    config = {
        "custom_fields": [
            {"name": "my_field", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "my_int", "duckdb_type": "BIGINT", "enabled": True},
            {"name": "my_disabled", "duckdb_type": "VARCHAR", "enabled": False},
        ]
    }
    hints = get_ingest_type_hints(config)
    assert hints["my_field"] == "VARCHAR" and hints["my_int"] == "BIGINT"
    assert "my_disabled" not in hints and hints["timestamp"] == "TIMESTAMPTZ"


def test_get_catalog_field_ids_with_custom_fields():
    config = {
        "custom_fields": [
            {"name": "zzz_field", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "aaa_field", "duckdb_type": "BIGINT", "enabled": True},
            {"name": "disabled_field", "duckdb_type": "VARCHAR", "enabled": False},
        ]
    }
    ids = get_catalog_field_ids(config)
    assert ids.index("aaa_field") < ids.index("zzz_field")
    assert "disabled_field" not in ids and "timestamp" in ids


def test_get_ingest_columns_sql_renders_duckdb_types_map():
    sql = get_ingest_columns_sql()
    assert sql.startswith("{") and sql.endswith("}") and "'timestamp': 'TIMESTAMPTZ'" in sql


def test_get_ingest_columns_sql_includes_enabled_custom_fields_only():
    cfg = {
        "custom_fields": [
            {"name": "extra_a", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "extra_disabled", "duckdb_type": "BIGINT", "enabled": False},
        ]
    }
    sql = get_ingest_columns_sql(cfg)
    assert "'extra_a': 'VARCHAR'" in sql and "extra_disabled" not in sql


def test_get_ingest_columns_sql_escapes_quote_in_field_metadata():
    cfg = {"custom_fields": [{"name": "hostile", "duckdb_type": "VARCHAR'; DROP TABLE", "enabled": True}]}
    sql = get_ingest_columns_sql(cfg)
    assert "VARCHAR''" in sql and "VARCHAR'; DROP" not in sql


def test_delete_objects_robust_returns_zero_for_empty_list():
    fake_s3 = MagicMock()
    assert _delete_objects_robust(fake_s3, "bkt", []) == 0
    fake_s3.delete_objects.assert_not_called()


def test_delete_objects_robust_uses_bulk_delete_for_happy_path():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {}
    assert _delete_objects_robust(fake_s3, "bkt", [f"k{i}" for i in range(20)]) == 20
    fake_s3.delete_objects.assert_called_once()
    payload = fake_s3.delete_objects.call_args.kwargs["Delete"]
    assert len(payload["Objects"]) == 20 and payload["Quiet"] is True


def test_delete_objects_robust_reports_exact_partial_failures():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {"Errors": [{"Key": "k2", "Code": "InternalError", "Message": "retry later"}]}
    assert _delete_objects_robust_with_failures(fake_s3, "bkt", ["k1", "k2", "k3"]) == (2, {"k2"})


def test_delete_objects_robust_batches_in_500_chunks():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {}
    assert _delete_objects_robust(fake_s3, "bkt", [f"k{i}" for i in range(1234)]) == 1234
    assert fake_s3.delete_objects.call_count == 3


def test_delete_objects_robust_short_circuits_on_access_denied_in_bulk():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {"Errors": [{"Code": "AccessDenied", "Message": "x"}]}
    assert _delete_objects_robust(fake_s3, "bkt", [f"k{i}" for i in range(10)]) == 0


def test_delete_objects_robust_falls_back_to_individual_on_bulk_exception():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.side_effect = RuntimeError("UnsupportedOperation")
    fake_s3.delete_object.return_value = {}
    assert _delete_objects_robust(fake_s3, "bkt", ["k1", "k2", "k3"]) == 3
    assert fake_s3.delete_object.call_count == 3


def test_delete_objects_robust_returns_zero_when_bulk_fails_with_access_denied():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.side_effect = RuntimeError("AccessDenied: not allowed")
    assert _delete_objects_robust(fake_s3, "bkt", ["k1", "k2"]) == 0
    fake_s3.delete_object.assert_not_called()


def test_delete_objects_robust_individual_fallback_stops_on_access_denied():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.side_effect = RuntimeError("bulk failed")
    call_count = {"n": 0}

    def _maybe_fail(*, Bucket, Key):
        call_count["n"] += 1
        if call_count["n"] >= 2:
            raise RuntimeError("AccessDenied")
        return {}

    fake_s3.delete_object.side_effect = _maybe_fail
    assert _delete_objects_robust(fake_s3, "bkt", ["k1", "k2", "k3", "k4"]) == 1
    assert call_count["n"] == 2


def test_delete_objects_robust_individual_fallback_continues_on_other_errors():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.side_effect = RuntimeError("bulk failed")
    call_count = {"n": 0}

    def _maybe_fail(*, Bucket, Key):
        call_count["n"] += 1
        if Key == "k2":
            raise RuntimeError("NoSuchKey: gone")
        return {}

    fake_s3.delete_object.side_effect = _maybe_fail
    assert _delete_objects_robust(fake_s3, "bkt", ["k1", "k2", "k3"]) == 2
    assert call_count["n"] == 3


@pytest.mark.parametrize(
    ("fname", "expected"),
    [
        ("2026-05-04T18-30-00.svc1234.gz", datetime(2026, 5, 4, 18, 30, 0, tzinfo=UTC)),
        ("2026-01-15T09-00-00.svc.gz", datetime(2026, 1, 15, 9, 0, 0, tzinfo=UTC)),
        ("2026-05-26T18:36:50.000-WFFKlnSyTWAMB3qwtVDD.log.gz", datetime(2026, 5, 26, 18, 36, 50, tzinfo=UTC)),
        ("manifest.json", None),
        ("readme.txt", None),
        (".DS_Store", None),
    ],
)
def test_parse_fastly_filename_dt(fname: str, expected: datetime | None):
    assert _parse_fastly_filename_dt(fname) == expected


@pytest.mark.parametrize(
    ("already", "lookback", "expected"),
    [
        (set(), 4, None),
        (
            {
                "s3://b/raw/2026-05-04/12/2026-05-04T12-00-00.svc.gz",
                "s3://b/raw/2026-05-04/15/2026-05-04T15-30-00.svc.gz",
            },
            4,
            "raw/2026-05-04/11/",
        ),
        ({"s3://b/raw/garbage-entry.gz", "s3://b/raw/another.json"}, 4, None),
        ({"s3://b/raw/2026-05-04/15/2026-05-04T15-00-00.svc.gz"}, 24, "raw/2026-05-03/15/"),
    ],
)
def test_compute_incremental_start_after(already: set[str], lookback: int, expected: str | None):
    assert _compute_incremental_start_after(already, lookback_hours=lookback) == expected


def _drain_ingest(gen):
    events = []
    try:
        for e in gen:
            events.append(e)
    except Exception as exc:
        return events, exc
    return events, None


@pytest.mark.parametrize(
    ("src", "needle"),
    [
        ({"name": "test-svc", "service_id": "svc-1", "access_level": "read_only", "bucket": "b"}, "read-only"),
        ({"name": "test-svc", "service_id": "svc-1", "bucket": ""}, "bucket"),
    ],
)
def test_ingest_yields_error_for_invalid_source(src: dict, needle: str):
    with patch("backend.core.ingest._ensure_source_registered"):
        events, _ = _drain_ingest(ingest.ingest(source=src))
    assert any(e.get("type") == "error" and needle in e["message"].lower() for e in events)


def test_ingest_short_circuits_to_done_when_no_new_files():
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = []
    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value = fake_paginator
    src = {"name": "test-svc", "service_id": "svc-1", "bucket": "b", "prefix": ""}
    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.metadata.get_ingested_filenames", return_value={"already1.gz", "already2.gz"}),
        patch("backend.core.ingest._get_fos_client", return_value=fake_s3),
    ):
        events, _ = _drain_ingest(ingest.ingest(source=src))
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None and done["new_files"] == 0 and done["skipped_files"] == 0


def test_ingest_yields_error_on_fos_list_failure():
    fake_s3 = MagicMock()
    fake_s3.get_paginator.side_effect = RuntimeError("S3 connection refused")
    src = {"name": "test-svc", "service_id": "svc-1", "bucket": "b", "prefix": ""}
    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
        patch("backend.core.ingest._get_fos_client", return_value=fake_s3),
    ):
        events, _ = _drain_ingest(ingest.ingest(source=src))
    error = next((e for e in events if e["type"] == "error"), None)
    assert error is not None and (
        "connection refused" in error["message"].lower() or "list" in error["message"].lower()
    )


def test_ingest_sets_force_download_on_mem_con():
    executed_sql: list[str] = []

    class _RecordingCon:
        def __init__(self, real):
            self._real = real

        def execute(self, sql, *args, **kwargs):
            executed_sql.append(sql)
            return self._real.execute(sql, *args, **kwargs)

        def close(self):
            return self._real.close()

        def __getattr__(self, name):
            return getattr(self._real, name)

    def _fake_get_mem_con(_src):
        con = duckdb.connect(":memory:")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        return _RecordingCon(con)

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "raw/2026-05-20/00/2026-05-20T00-00-00.000.gz", "Size": 1024}]}
    ]
    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value = fake_paginator
    src = {"name": "test-svc", "service_id": "svc-1", "bucket": "b", "prefix": ""}
    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
        patch("backend.core.ingest._get_fos_client", return_value=fake_s3),
        patch("backend.core.duckdb.get_memory_connection", side_effect=_fake_get_mem_con),
        patch("backend.core.ingest._execute_query_with_retry", side_effect=RuntimeError("synthetic stop")),
    ):
        _drain_ingest(ingest.ingest(source=src))
    normalized = [" ".join(s.split()).lower() for s in executed_sql]
    assert any("set force_download = true" in s for s in normalized)


def _src(name: str, akid: str, *, cdn_secret: str = "") -> dict:
    return {
        "name": name,
        "service_id": name,
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "endpoint": "https://us-east-1.object.fastlystorage.app",
        "access_key_id": akid,
        "secret_access_key": "secret",
        "cdn_url": "https://cdn.example.test",
        "cdn_secret": cdn_secret,
    }


def _reset_cache() -> None:
    with _ddb._fos_client_lock:
        _ddb._fos_client_cache.clear()


def test_same_creds_reuse_cached_client():
    _reset_cache()
    try:
        assert _ddb._get_fos_client(_src("svc-a", "KEY1")) is _ddb._get_fos_client(_src("svc-a", "KEY1"))
    finally:
        _reset_cache()


def test_rotated_key_misses_cache():
    _reset_cache()
    try:
        c_old = _ddb._get_fos_client(_src("svc-rot", "OLDKEY"))
        c_new = _ddb._get_fos_client(_src("svc-rot", "NEWKEY"))
        assert c_old is not c_new
        keys = {k[:2] for k in _ddb._fos_client_cache if k[0] == "svc-rot"}
        assert ("svc-rot", "OLDKEY") in keys and ("svc-rot", "NEWKEY") in keys
    finally:
        _reset_cache()


def test_rotated_cdn_secret_misses_cache():
    _reset_cache()
    try:
        c_old = _ddb._get_fos_client(_src("svc-cdn-rot", "KEY1", cdn_secret="OLDSECRET"))
        c_new = _ddb._get_fos_client(_src("svc-cdn-rot", "KEY1", cdn_secret="NEWSECRET"))
        assert c_old is not c_new
    finally:
        _reset_cache()


def test_clear_fos_client_drops_all_entries_for_name():
    _reset_cache()
    try:
        _ddb._get_fos_client(_src("svc-clear", "OLDKEY"))
        _ddb._get_fos_client(_src("svc-clear", "NEWKEY"))
        _ddb._get_fos_client(_src("other", "K"))
        _ddb.clear_fos_client("svc-clear")
        remaining = set(_ddb._fos_client_cache)
        assert not any(k[0] == "svc-clear" for k in remaining)
        assert any(k[:2] == ("other", "K") for k in remaining)
        _ddb.clear_fos_client({"name": "other"})
        assert not any(k[:2] == ("other", "K") for k in _ddb._fos_client_cache)
    finally:
        _reset_cache()


def test_clear_fos_client_is_noop_when_absent():
    _reset_cache()
    _ddb.clear_fos_client("never-built")
    _ddb.clear_fos_client({"name": "never-built"})


def test_deterministic_buffer_name_is_stable_and_distinct():
    name1 = ingest._deterministic_buffer_name(["s3://b/a.gz", "s3://b/b.gz"])
    name2 = ingest._deterministic_buffer_name(["s3://b/b.gz", "s3://b/a.gz"])
    assert name1 == name2 and name1.startswith("batch_") and name1.endswith(".parquet")
    assert len(name1) == len("batch_") + 16 + len(".parquet")
    assert ingest._deterministic_buffer_name(["s3://b/a.gz"]) != ingest._deterministic_buffer_name(["s3://b/b.gz"])


def test_record_list_overwrite_and_clear_in_flight():
    sid = "svc-inflight-roundtrip"
    rows = [("s3://b/a.gz", 100, 4096), ("s3://b/b.gz", 200, 8192)]
    metadata_db.record_in_flight(sid, "batch_abc.parquet", rows)
    assert metadata_db.list_in_flight(sid) == [("batch_abc.parquet", rows)]

    sid_u = "svc-inflight-upsert"
    metadata_db.record_in_flight(sid_u, "batch_xyz.parquet", [("a", 1, 10)])
    metadata_db.record_in_flight(sid_u, "batch_xyz.parquet", [("a", 1, 10), ("b", 2, 20)])
    assert metadata_db.list_in_flight(sid_u) == [("batch_xyz.parquet", [("a", 1, 10), ("b", 2, 20)])]

    sid_c = "svc-inflight-clear"
    metadata_db.record_in_flight(sid_c, "batch_clr.parquet", [("a", 1, 10)])
    metadata_db.clear_in_flight(sid_c, "batch_clr.parquet")
    metadata_db.clear_in_flight(sid_c, "batch_clr.parquet")
    assert metadata_db.list_in_flight(sid_c) == []


def test_recover_in_flight_promotes_when_buffer_exists(tmp_path):
    sid = "svc-recover-promote"
    metadata_db.record_in_flight(sid, "batch_present.parquet", [("s3://b/x.gz", 50, 1000), ("s3://b/y.gz", 75, 2000)])
    buf_dir = tmp_path / "buffer"
    buf_dir.mkdir()
    (buf_dir / "batch_present.parquet").write_bytes(b"PAR1stub")
    with patch("backend.core.iceberg._buffer_dir", return_value=str(buf_dir)):
        result = ingest._recover_in_flight({"name": sid})
    assert result == {"promoted": 1, "dropped": 0, "rows_recovered": 125}
    assert metadata_db.get_ingested_filenames(sid) == {"s3://b/x.gz", "s3://b/y.gz"}
    assert metadata_db.list_in_flight(sid) == []


def test_recover_in_flight_drops_when_buffer_missing(tmp_path):
    sid = "svc-recover-drop"
    metadata_db.record_in_flight(sid, "batch_missing.parquet", [("s3://b/q.gz", 10, 100)])
    buf_dir = tmp_path / "buffer"
    buf_dir.mkdir()
    with patch("backend.core.iceberg._buffer_dir", return_value=str(buf_dir)):
        result = ingest._recover_in_flight({"name": sid})
    assert result["promoted"] == 0 and result["dropped"] == 1
    assert metadata_db.get_ingested_filenames(sid) == set() and metadata_db.list_in_flight(sid) == []


def test_recover_in_flight_is_noop_when_no_pending(tmp_path):
    with patch("backend.core.iceberg._buffer_dir", return_value=str(tmp_path)):
        assert ingest._recover_in_flight({"name": "svc-recover-empty"}) == {
            "promoted": 0,
            "dropped": 0,
            "rows_recovered": 0,
        }


def test_recover_in_flight_handles_mixed_state(tmp_path):
    sid = "svc-recover-mixed"
    metadata_db.record_in_flight(sid, "batch_here.parquet", [("a.gz", 5, 50)])
    metadata_db.record_in_flight(sid, "batch_gone.parquet", [("b.gz", 7, 70)])
    buf_dir = tmp_path / "buffer"
    buf_dir.mkdir()
    (buf_dir / "batch_here.parquet").write_bytes(b"PAR1stub")
    with patch("backend.core.iceberg._buffer_dir", return_value=str(buf_dir)):
        result = ingest._recover_in_flight({"name": sid})
    assert result == {"promoted": 1, "dropped": 1, "rows_recovered": 5}
    assert metadata_db.get_ingested_filenames(sid) == {"a.gz"} and metadata_db.list_in_flight(sid) == []


def test_list_in_flight_is_per_service():
    metadata_db.record_in_flight("svc-inflight-a", "batch_a.parquet", [("a", 1, 10)])
    metadata_db.record_in_flight("svc-inflight-b", "batch_b.parquet", [("b", 2, 20)])
    a_pending = metadata_db.list_in_flight("svc-inflight-a")
    b_pending = metadata_db.list_in_flight("svc-inflight-b")
    assert len(a_pending) == 1 and a_pending[0][0] == "batch_a.parquet"
    assert len(b_pending) == 1 and b_pending[0][0] == "batch_b.parquet"


def test_recover_in_flight_survives_buffer_dir_missing(tmp_path):
    sid = "svc-recover-no-dir"
    metadata_db.record_in_flight(sid, "batch_nodir.parquet", [("a.gz", 1, 1)])
    missing_dir = str(tmp_path / "does_not_exist" / "buffer")
    assert not os.path.exists(missing_dir)
    with patch("backend.core.iceberg._buffer_dir", return_value=missing_dir):
        result = ingest._recover_in_flight({"name": sid})
    assert result["dropped"] == 1 and result["promoted"] == 0


_INGEST_WRAPPER_SERVICE_ID = "test-ingest-task-wrappers"


def _depths(depth: int, ok: bool = True):
    return patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": depth}, ok))


@pytest.mark.parametrize(
    ("task_attr", "target_fn", "other_fn", "keys"),
    [
        (
            "convert_batch_files",
            "convert_batch_objects",
            "convert_batch_rum_objects",
            ["raw/a.json.gz", "raw/b.json.gz", "raw/c.json.gz"],
        ),
        (
            "convert_batch_rum_files",
            "convert_batch_rum_objects",
            "convert_batch_objects",
            ["raw/rum/a.json.gz", "raw/rum/b.json.gz"],
        ),
    ],
)
def test_convert_batch_task_routes_to_correct_converter(task_attr: str, target_fn: str, other_fn: str, keys: list[str]):
    task = getattr(ingest, task_attr)
    with (
        patch.object(ingest, target_fn, return_value={"committed": len(keys)}) as mock_target,
        patch.object(ingest, other_fn) as mock_other,
    ):
        assert task.apply(args=(_INGEST_WRAPPER_SERVICE_ID, keys)).get() == {"committed": len(keys)}
    mock_target.assert_called_once()
    assert mock_target.call_args.args[:2] == (_INGEST_WRAPPER_SERVICE_ID, keys) and mock_target.call_args.args[2]
    mock_other.assert_not_called()


def test_every_ledger_task_is_registered_under_its_module_path():
    for name, task in {
        "backend.core.ingest.convert_batch_files": ingest.convert_batch_files,
        "backend.core.ingest.convert_batch_rum_files": ingest.convert_batch_rum_files,
    }.items():
        assert task.name == name and name in ingest.app.tasks and ingest.app.tasks[name].name == name


def test_celery_ledger_claim_semantics():
    service_id, object_key = "test-celery-svc", "raw/2026/08/27/10/05/logs.json.gz"
    con = get_con(service_id)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id=?", (service_id,))
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by) VALUES (?, ?, 'claimed', 'worker-1')",
        (service_id, object_key),
    )
    con.commit()
    with patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test"}):
        ingest.convert_batch_files.push_request(id="worker-2")
        try:
            ingest.convert_batch_files(service_id, [object_key])
        finally:
            ingest.convert_batch_files.pop_request()
    cur.execute(
        "SELECT status, claimed_by FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, object_key)
    )
    row = cur.fetchone()
    assert row["status"] == "claimed" and row["claimed_by"] == "worker-1"


@pytest.mark.parametrize("label", ["", "RUM "])
def test_dispatches_reclaimed_then_stuck_in_batches(label: str) -> None:
    dispatch = MagicMock()
    n = ingest.LEDGER_CONVERT_BATCH_SIZE
    reclaimed, stuck = [f"r{i}" for i in range(3)], [f"s{i}" for i in range(n)]
    with _depths(0):
        assert ingest._redispatch_pending("svc", reclaimed, stuck, dispatch, label) == (n + 3, True)
    sent = [c.args for c in dispatch.call_args_list]
    assert all(a[0] == "svc" for a in sent)
    assert [k for a in sent for k in a[1]] == reclaimed + stuck
    assert [len(a[1]) for a in sent] == [n, 3]


def test_redispatch_pending_skip_conditions() -> None:
    dispatch = MagicMock()
    with _depths(10):
        assert ingest._redispatch_pending("svc", [], ["a"], dispatch, "") == (0, True)
    with _depths(0, ok=False):
        assert ingest._redispatch_pending("svc", ["a"], [], dispatch, "RUM ") == (0, False)
    with patch("backend.celery_status.celery_queue_depths", side_effect=RuntimeError("boom")):
        assert ingest._redispatch_pending("svc", ["a"], [], dispatch, "") == (0, False)
    assert ingest._redispatch_pending("svc", [], [], dispatch, "") == (0, True)
    dispatch.assert_not_called()


@pytest.mark.parametrize(
    ("sweep_name", "delay_name", "prefix"),
    [
        ("sweep_ledger_once", "convert_batch_files", "raw/2026/08/27"),
        ("sweep_rum_ledger_once", "convert_batch_rum_files", "raw/rum/2026/08/27"),
    ],
)
def test_sweep_dispatches_each_pending_key_once_with_5000_rows(sweep_name: str, delay_name: str, prefix: str) -> None:
    service_id = "test-celery-svc"
    con, cur = clear_ledger(service_id)
    now = time.time()
    stale_claim, old = now - ingest.LEDGER_RECLAIM_AFTER_S - 60, now - 3600
    rows = [
        (service_id, f"{prefix}/claimed_{i:05d}.json.gz", "claimed", "dead", stale_claim, old) for i in range(2500)
    ] + [(service_id, f"{prefix}/stuck_{i:05d}.json.gz", "discovered", None, None, old) for i in range(2500)]
    cur.executemany(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    con.commit()

    def _no_files(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}

    with (
        patch("backend.config.load_config", return_value={"service_id": service_id}),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "b"}),
        patch("backend.core.ingest.list_fos_files", side_effect=_no_files),
        patch("backend.core.ingest.discover_prefix", return_value=0),
        patch("backend.core.ingest.discover_rum_prefix", return_value=0),
        patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
        patch(f"backend.core.ingest.{delay_name}.delay") as delay,
    ):
        summary = getattr(ingest, sweep_name)(service_id)

    keys = [k for c in delay.call_args_list for k in c.args[1]]
    assert summary["reclaimed"] == 2500 and summary["redispatched"] == 5000
    assert len(keys) == len(set(keys)) == 5000
    assert delay.call_count == -(-5000 // ingest.LEDGER_CONVERT_BATCH_SIZE)


_make_source = partial(
    make_source,
    kind="cloud",
    cdn_url="",
    cdn_secret="",
    cdn_service_id="",
    logging_service_id="test",
    duckdb_path=":memory:",
    log_period=60,
    provisioning={},
)


def _drain(gen) -> list[dict]:
    return list(gen)


class TestIngestReadOnlyGuard:
    def test_read_only_yields_error_immediately(self):
        events = _drain(ingest.ingest(source=_make_source(access_level="read_only")))
        assert len(events) == 1 and events[0]["type"] == "error" and "read-only" in events[0]["message"].lower()

    def test_read_write_does_not_error_on_access_level(self):
        with patch("backend.core.ingest._ensure_source_registered", side_effect=ValueError("stop")):
            events, _ = _drain_ingest(ingest.ingest(source=_make_source(access_level="read_write")))
        assert not any(e.get("type") == "error" and "read-only" in e.get("message", "").lower() for e in events)


class TestIngestStartTimeExec:
    def test_start_time_exec_is_float(self):
        call_count = {"n": 0}
        real_start = time.time()

        def fake_time():
            call_count["n"] += 1
            return real_start if call_count["n"] == 1 else real_start + 999.0

        fake_file = "raw/2026-01-01/00/2026-01-01T00-00-00.svc.gz"
        with (
            patch("backend.core.ingest._ensure_source_registered"),
            patch("backend.core.ingest._get_fos_client") as mock_fos,
            patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
            patch("backend.core.metadata.insert_ingested_files"),
            patch("time.time", side_effect=fake_time),
        ):
            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = iter([{"Contents": [{"Key": fake_file, "Size": 100}]}])
            mock_fos.return_value.get_paginator.return_value = mock_paginator
            events = list(ingest.ingest(source=_make_source(), max_seconds=1, start_time="2026-01-01T00:00:00Z"))
        assert not any("unsupported operand" in e.get("message", "") for e in events if e.get("type") == "error")


class TestIngestMaxSeconds:
    def test_expired_listing_budget_still_attempts_first_batch(self):
        fake_file = "s3://bucket/raw/request/year=2026/month=01/day=01/hour=00/minute=00/test.log.gz"

        def fake_list_fos_files(**_kwargs):
            yield {"type": "status", "message": "Discovering"}
            return {
                "new_files": [fake_file],
                "file_sizes": {fake_file: 100},
                "skipped_already": 0,
                "stranded_already": [],
            }

        with (
            patch("backend.core.ingest._ensure_source_registered"),
            patch("backend.core.ingest._recover_in_flight", return_value={"promoted": 0, "dropped": 0}),
            patch("backend.core.ingest._get_fos_client"),
            patch("backend.core.ingest.metadata_db.get_ingested_filenames", return_value=set()),
            patch("backend.core.ingest.list_fos_files", side_effect=fake_list_fos_files),
            patch("backend.core.duckdb.get_memory_connection", return_value=MagicMock()),
            patch("backend.core.ingest._download_chunk_to_local", return_value=({}, {})) as download,
            patch("backend.config.load_config", return_value={}),
            patch("backend.core.ingest.time.time", side_effect=[1_000.0, 1_999.0, 1_999.0, 1_999.0]),
        ):
            events = _drain(ingest.ingest(source=_make_source(), max_seconds=240, incremental_only=True))
        download.assert_called_once()
        assert any("Processing 1 files in batch 1/1" in event.get("message", "") for event in events)

    def test_max_seconds_zero_and_type_annotations(self):
        events = _drain(ingest.ingest(source=_make_source(access_level="read_only"), max_seconds=0))
        assert events[0]["type"] == "error"
        sig = inspect.signature(ingest.ingest)
        max_s = str(sig.parameters["max_seconds"].annotation)
        start_t = str(sig.parameters["start_time"].annotation)
        assert "int" in max_s and "None" in max_s
        assert "str" in start_t and "None" in start_t and "float" not in start_t


def _run_workers(targets):
    threads = [threading.Thread(target=t, args=a) for t, a in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive(), "worker thread hung past 30s"


def test_concurrent_ingest_calls_one_wins_one_is_no_op(ingest_local_env, monkeypatch):  # noqa: F811
    log_dir, src = ingest_local_env["log_dir"], ingest_local_env["src"]
    service_id = src["name"]
    key = "raw/2026-06-10/08/2026-06-10T08-00-00.solo.gz"
    _seed_local(log_dir, key, "2026-06-10T08:00:00Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[key])
    barrier, results, lock = threading.Barrier(2), {}, threading.Lock()

    def worker(label: str) -> None:
        barrier.wait()
        outcome: dict = {"label": label}
        try:
            run_id = metadata_db.start_cron_run(service_id, "sync")
            outcome["run_id"], outcome["winner"] = run_id, True
            try:
                events = _drain(ingest.ingest(source=src))
                done = next((e for e in events if e["type"] == "done"), None)
                outcome["rows_inserted"] = done.get("rows_inserted") if done else None
            finally:
                try:
                    metadata_db.log_cron_run(service_id, "sync", 0.0, "success", run_id=run_id)
                except Exception:
                    pass
        except RuntimeError as exc:
            outcome["winner"], outcome["error"] = False, str(exc)
        with lock:
            results[label] = outcome

    _run_workers([(worker, ("a",)), (worker, ("b",))])
    winners = [r for r in results.values() if r.get("winner")]
    losers = [r for r in results.values() if not r.get("winner")]
    assert len(winners) == 1 and len(losers) == 1
    assert "already running" in losers[0]["error"].lower() and winners[0]["rows_inserted"] == 1
    assert len([n for n in metadata_db.get_ingested_filenames(service_id) if "solo.gz" in n]) == 1


def test_concurrent_ingest_on_different_sources_both_succeed(ingest_local_env, monkeypatch):  # noqa: F811
    log_dir = ingest_local_env["log_dir"]
    src_a = dict(ingest_local_env["src"])
    src_b = {**src_a, "name": "test_service_b", "service_id": "test-service-b"}
    bucket_prefix, file_prefix = f"s3://{src_a['bucket']}/", "file://" + str(log_dir.absolute()) + "/"

    monkeypatch.setattr(
        _ddb,
        "get_memory_connection",
        lambda _src: _S3RewritingConn(duckdb.connect(":memory:"), bucket_prefix, file_prefix),
    )
    monkeypatch.setattr(
        _ddb, "_cache_dir", lambda s: str(log_dir.parent / "cache" / str(s.get("service_id") or s["name"]))
    )

    key_a = "raw/2026-06-11/09/2026-06-11T09-00-00.a.gz"
    key_b = "raw/2026-06-11/09/2026-06-11T09-00-00.b.gz"
    _seed_local(log_dir, key_a, "2026-06-11T09:00:00Z")
    _seed_local(log_dir, key_b, "2026-06-11T09:00:00Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[key_a, key_b])

    barrier, results, lock = threading.Barrier(2), {}, threading.Lock()

    def worker(label: str, src: dict) -> None:
        barrier.wait()
        outcome: dict = {"label": label, "service_id": src["name"]}
        try:
            run_id = metadata_db.start_cron_run(src["name"], "sync")
            outcome["run_id"], outcome["winner"] = run_id, True
            events = _drain(ingest.ingest(source=src))
            done = next((e for e in events if e["type"] == "done"), None)
            outcome["rows_inserted"] = done.get("rows_inserted") if done else None
            try:
                metadata_db.log_cron_run(src["name"], "sync", 0.0, "success", run_id=run_id)
            except Exception:
                pass
        except RuntimeError as exc:
            outcome["winner"], outcome["error"] = False, str(exc)
        with lock:
            results[label] = outcome

    _run_workers([(worker, ("a", src_a)), (worker, ("b", src_b))])
    for r in results.values():
        assert r.get("winner") is True and r["rows_inserted"] >= 1
    assert metadata_db.get_ingested_filenames(src_a["name"]) and metadata_db.get_ingested_filenames(src_b["name"])


_buffer_names = st.text(alphabet=string.ascii_lowercase, min_size=3, max_size=8).map(lambda s: f"batch_{s}.parquet")
_file_names = st.text(alphabet=string.ascii_lowercase + string.digits, min_size=3, max_size=8).map(
    lambda s: f"raw/2026-06-17/00/{s}.gz"
)
_file_manifest = st.tuples(
    _file_names,
    st.integers(min_value=0, max_value=10_000),
    st.one_of(st.none(), st.integers(min_value=0, max_value=1_000_000)),
)
_file_manifests = st.lists(_file_manifest, min_size=1, max_size=4, unique_by=lambda t: t[0])


class IngestMetadataStateMachine(RuleBasedStateMachine):
    buffers = Bundle("buffers")

    @initialize()
    def setup(self):
        self.service_id = "svc-hypothesis-ingest"
        con = metadata_db.get_con(self.service_id)
        for tbl in ("ingest_in_flight", "ingested_files", "ingested_files_summary"):
            con.execute(f"DELETE FROM {tbl} WHERE source_name = ?", (self.service_id,))
        con.commit()
        metadata_db.teardown(self.service_id)
        self.shadow_in_flight: dict[str, list[tuple[str, int, int | None]]] = {}
        self.shadow_ingested: set[str] = set()

    @rule(target=buffers, buffer=_buffer_names, files=_file_manifests)
    def record_in_flight(self, buffer, files):
        files_t = [tuple(f) for f in files]
        metadata_db.record_in_flight(self.service_id, buffer, files_t)
        self.shadow_in_flight[buffer] = files_t
        return buffer

    @rule(buffer=consumes(buffers))
    def promote_and_clear(self, buffer):
        files = self.shadow_in_flight.pop(buffer, None)
        if files is None:
            return
        metadata_db.insert_ingested_files(self.service_id, files)
        metadata_db.clear_in_flight(self.service_id, buffer)
        self.shadow_ingested.update(fname for (fname, _, _) in files)

    @rule(buffer=consumes(buffers))
    def crash_drop(self, buffer):
        self.shadow_in_flight.pop(buffer, None)
        metadata_db.clear_in_flight(self.service_id, buffer)

    @rule(buffer=_buffer_names)
    def idempotent_clear(self, buffer):
        if buffer not in self.shadow_in_flight:
            metadata_db.clear_in_flight(self.service_id, buffer)

    @invariant()
    def in_flight_matches_shadow(self):
        db_state = {buffer: list(files) for buffer, files in metadata_db.list_in_flight(self.service_id)}
        assert set(db_state.keys()) == set(self.shadow_in_flight.keys())
        for buffer, shadow_files in self.shadow_in_flight.items():
            assert db_state[buffer] == shadow_files

    @invariant()
    def ingested_matches_shadow(self):
        assert metadata_db.get_ingested_filenames(self.service_id) == self.shadow_ingested

    @invariant()
    def rollup_count_matches_shadow(self):
        con = metadata_base.get_con(self.service_id)
        row = con.execute(
            "SELECT file_count FROM ingested_files_summary WHERE source_name = ?", (self.service_id,)
        ).fetchone()
        assert (row["file_count"] if row else 0) == len(self.shadow_ingested)


TestIngestMetadataStateMachine = IngestMetadataStateMachine.TestCase
TestIngestMetadataStateMachine.settings = settings(
    max_examples=20,
    deadline=None,
    stateful_step_count=15,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
