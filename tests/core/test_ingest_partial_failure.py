"""Targeted partial-failure tests for :mod:`backend.core.ingest`.

The happy path is well-covered by ``test_ingest.py`` and the single-
corrupt-file path by ``test_ingest_corruption.py``. The audit identified
several *partial-failure* branches that were untested despite being the
exact branches that fire during real S3 / chunk-download flakes.

These tests reuse the same local-fs S3 substitute as
``test_ingest_corruption.py``: a MockFos client serves from a tmp dir,
and the in-memory DuckDB connection rewrites ``s3://bucket/...`` paths
to ``file://local/...`` so the httpfs reads land on local disk.

Specifically pins:
  1. Isolation-retry total failure: every file in the batch unreadable
     → no buffer written, no metadata rows, no orphan in_flight.
  2. S3 deletion race during chunk download: one file disappears
     between LIST and GET → the disappearing file is skipped, the
     survivors still ingest cleanly.
  3. end_time filter excludes future-dated rows from the buffer (the
     filename passes the discovery filter but the row-timestamp gate
     drops anything past end_time).
"""

from __future__ import annotations

import gzip
import io
import json

import botocore.exceptions
import pytest

from backend.core.ingest import ingest


def _drain(gen):
    return list(gen)


def _seed_local(log_dir, key, ts_iso, status=200, n_rows=1):
    path = log_dir / key
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {"timestamp": ts_iso, "ip": f"10.0.0.{i}", "status": status, "url": f"/p/{i}", "method": "GET"}
        for i in range(n_rows)
    ]
    with gzip.open(path, "wt") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    return path


class _S3RewritingConn:
    """Wrap a DuckDB connection so SQL with ``s3://bucket/...`` paths
    is rewritten to ``file://local/...`` before execution. Same pattern
    as test_ingest_corruption.py."""

    def __init__(self, conn, bucket_prefix: str, file_prefix: str):
        self._conn = conn
        self._bucket_prefix = bucket_prefix
        self._file_prefix = file_prefix

    def execute(self, sql, *args, **kwargs):
        if isinstance(sql, str) and self._bucket_prefix in sql:
            sql = sql.replace(self._bucket_prefix, self._file_prefix)
        return self._conn.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _install_mock_fos(monkeypatch, log_dir, *, keys, get_object_handler=None, valid_size=100):
    """Wire a MockFos client + redirect cache + reroute SQL to local files."""

    class MockPaginator:
        def paginate(self, **_kwargs):
            return [
                {"Contents": [{"Key": k, "Size": valid_size} for k in keys]},
            ]

    class MockFos:
        def get_paginator(self, *_args, **_kwargs):
            return MockPaginator()

        def get_object(self, Bucket, Key):  # noqa: N803
            if get_object_handler is not None:
                resp = get_object_handler(Bucket, Key)
                if resp is not None:
                    return resp
            with open(log_dir / Key, "rb") as fh:
                return {"Body": io.BytesIO(fh.read())}

        def delete_objects(self, **_kwargs):
            return {}

    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda *_a: MockFos())


def _route_duckdb_to_local(monkeypatch, in_memory_duckdb, bucket, log_dir):
    """Rewrite s3://bucket/... → file://logdir/... inside DuckDB SQL."""
    bucket_prefix = f"s3://{bucket}/"
    file_prefix = "file://" + str(log_dir.absolute()) + "/"
    import backend.core.duckdb as my_duckdb

    monkeypatch.setattr(
        my_duckdb,
        "get_memory_connection",
        lambda src: _S3RewritingConn(in_memory_duckdb, bucket_prefix, file_prefix),
    )


@pytest.fixture
def ingest_local_env(monkeypatch, fos_source, in_memory_duckdb, tmp_path):
    """Common setup: redirect cache, disable FOS proxy + DuckDB rewrite,
    small chunk size so isolation kicks in."""
    log_dir = tmp_path / "mock_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _: str(tmp_path / "cache"))
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *_a: None)
    monkeypatch.setattr("time.sleep", lambda *_a: None)
    monkeypatch.setattr("backend.core.duckdb.INGEST_CHUNK_SIZE", 10)

    _route_duckdb_to_local(monkeypatch, in_memory_duckdb, fos_source["bucket"], log_dir)

    return {"log_dir": log_dir, "src": {**fos_source}, "duckdb": in_memory_duckdb}


def test_isolation_retry_total_failure_writes_no_buffer_no_metadata(ingest_local_env, monkeypatch):
    """Every file in the batch is corrupt → the isolation loop walks each
    one, finds all unreadable, yields 'All files in batch are unreadable',
    and ``continue``s without writing a buffer. Pin three contracts:

      1. No buffer parquet appears under cache/buffer/
      2. metadata_db.ingested_files stays empty
      3. ingest_in_flight stays empty (no orphan rows)

    Without this test, a regression that erroneously called
    insert_ingested_files on the all-failure path would silently mark
    files 'ingested' that never had their rows persisted — permanent
    silent data loss until the file's retention window expired."""
    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]

    # Two files, both non-gzip garbage — both will fail DuckDB read_json_auto.
    for key in ("raw/2026-06-01/10/2026-06-01T10-00-00.bad1.gz", "raw/2026-06-01/10/2026-06-01T10-05-00.bad2.gz"):
        p = log_dir / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"not a gzip at all")

    _install_mock_fos(
        monkeypatch,
        log_dir,
        keys=[
            "raw/2026-06-01/10/2026-06-01T10-00-00.bad1.gz",
            "raw/2026-06-01/10/2026-06-01T10-05-00.bad2.gz",
        ],
    )

    events = _drain(ingest(source=src))

    done = next(e for e in events if e["type"] == "done")
    assert done["rows_inserted"] == 0

    from backend.core import iceberg
    from backend.core import metadata as metadata_db

    assert iceberg.buffer_files(src) == [], (
        "isolation-retry-total-failure path wrote a buffer parquet; "
        "no rows survived isolation so no buffer should exist."
    )
    assert metadata_db.get_ingested_filenames(src["name"]) == set(), (
        "isolation-retry-total-failure path recorded files in metadata_db; "
        "without a successful ingest there is nothing to commit and these "
        "would become silent 'ingested but not actually persisted' ghosts."
    )
    assert metadata_db.list_in_flight(src["name"]) == [], (
        "ingest_in_flight non-empty after all-files-failed path; the "
        "in_flight row would re-fire _recover_in_flight forever."
    )


def test_chunk_get_object_failure_skips_just_that_file(ingest_local_env, monkeypatch):
    """boto3 ``get_object`` raises NoSuchKey for one file → the
    downloader records that file as failed and proceeds with the rest.
    The healthy file still lands in the buffer + metadata; the missing
    one does NOT.

    This pins the S3 deletion race: a file LISTed at T0 can be
    gone by the time the per-chunk GET fires at T1 (lifecycle policy,
    Fastly internal cleanup, etc.). The cron must not abort the whole
    tick over a single missing file."""
    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]

    good_key = "raw/2026-06-01/11/2026-06-01T11-00-00.ok.gz"
    gone_key = "raw/2026-06-01/11/2026-06-01T11-05-00.gone.gz"
    _seed_local(log_dir, good_key, "2026-06-01T11:00:00Z")
    # gone_key is intentionally NOT created — get_object will raise.

    def _flaky_get(Bucket, Key):  # noqa: N803
        if Key == gone_key:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "key vanished"}}, "GetObject"
            )
        return None  # fall through to default

    _install_mock_fos(monkeypatch, log_dir, keys=[good_key, gone_key], get_object_handler=_flaky_get)

    events = _drain(ingest(source=src))

    done = next(e for e in events if e["type"] == "done")
    # Only the healthy file's row landed.
    assert done["rows_inserted"] == 1

    from backend.core import metadata as metadata_db

    ingested = metadata_db.get_ingested_filenames(src["name"])
    assert any(good_key in name for name in ingested), (
        f"good file {good_key!r} missing from metadata_db.ingested_files: {ingested}"
    )
    assert not any(gone_key in name for name in ingested), (
        f"file that vanished from S3 ({gone_key!r}) ended up in "
        "metadata_db.ingested_files — the downloader's failed_paths "
        "tracking failed to filter it out."
    )


def test_recover_in_flight_runs_at_start_of_every_ingest(ingest_local_env, monkeypatch):
    """A previous tick left an orphan in_flight row with a buffer file
    present (crashed AFTER write_to_buffer, BEFORE insert_ingested_files).
    The next ingest call must run _recover_in_flight FIRST, promote
    those rows into ingested_files, and clear the orphan in_flight
    BEFORE attempting to discover any new files.

    Pins the contract that crash-recovery is not deferred to a separate
    job — it's inline at the top of every ingest tick. A refactor that
    moved it to a manual admin endpoint would let orphan in_flights
    accumulate across restarts."""
    from backend.core import iceberg
    from backend.core import metadata as metadata_db

    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]

    # Plant a fake buffer parquet + matching in_flight row, as if a prior
    # ingest crashed between write_to_buffer and insert_ingested_files.
    buf_dir = iceberg._buffer_dir(src)  # type: ignore[attr-defined]
    import os

    os.makedirs(buf_dir, exist_ok=True)
    orphan_buffer = "batch_orphan_deadbeef.parquet"
    open(os.path.join(buf_dir, orphan_buffer), "wb").write(b"placeholder")
    orphan_rows = [
        ("s3://test-bucket/raw/2026-05-30/10/2026-05-30T10-00-00.orphan.gz", 42, 100),
    ]
    metadata_db.record_in_flight(src["name"], orphan_buffer, orphan_rows)

    # No new files to discover — the ingest must still run recovery.
    _install_mock_fos(monkeypatch, log_dir, keys=[])

    _drain(ingest(source=src))

    # After ingest: the orphan in_flight is gone AND ingested_files has
    # the promoted entry.
    assert metadata_db.list_in_flight(src["name"]) == [], (
        "_recover_in_flight failed to clear the orphan row — the next "
        "ingest will re-recover and the in_flight table grows unbounded."
    )
    ingested = metadata_db.get_ingested_filenames(src["name"])
    assert any("orphan.gz" in n for n in ingested), f"recovery did not promote the in_flight rows; ingested={ingested}"


def test_executor_submit_failure_falls_back_to_inline_delete(ingest_local_env, monkeypatch):
    """``concurrent.futures.thread._shutdown`` is process-wide and one-way.
    Once it flips (uvicorn worker recycling, multiprocessing libraries,
    bpython-style hot reload), every subsequent ThreadPoolExecutor.submit
    raises ``RuntimeError: cannot schedule new futures after interpreter
    shutdown`` — even on a freshly-constructed executor.

    Without the inline-delete fallback, the ingest loop crashes mid-chunk
    AFTER ``write_to_buffer`` + ``insert_ingested_files`` have already
    persisted the rows. The .gz files stay in FOS forever as orphans
    because the dedup gate at the top of every subsequent ingest sees
    them in ``ingested_files`` and skips them. Observed 2026-06-16:
    167 orphans across 4 hours of late-arriving Fastly drops.

    Pin: a forced submit failure must fall back to a synchronous
    ``_delete_objects_robust`` call so no orphans leak, and ingest must
    still report ``done`` instead of crashing.
    """
    import concurrent.futures

    from backend.core import ingest as ingest_module
    from backend.core import metadata as metadata_db

    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]

    key = "raw/2026-06-01/12/2026-06-01T12-00-00.ok.gz"
    _seed_local(log_dir, key, "2026-06-01T12:00:00Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[key])

    # Simulate the post-interpreter-shutdown state — but ONLY for the
    # delete executor. The chunk-download path also uses a ThreadPoolExecutor;
    # breaking that one too would short-circuit ingest before any delete
    # path is reached. Targeting by thread_name_prefix isolates the failure
    # to the exact submit site this regression test cares about.
    original_submit = concurrent.futures.ThreadPoolExecutor.submit

    def _selectively_broken_submit(self, *args, **kwargs):
        if getattr(self, "_thread_name_prefix", "") == "ingest_delete":
            raise RuntimeError("cannot schedule new futures after interpreter shutdown")
        return original_submit(self, *args, **kwargs)

    monkeypatch.setattr(concurrent.futures.ThreadPoolExecutor, "submit", _selectively_broken_submit)

    # Track inline-delete invocations to prove the fallback fired.
    inline_delete_calls: list[list[str]] = []
    original_delete = ingest_module._delete_objects_robust

    def _tracking_delete(client, bucket, keys):
        inline_delete_calls.append(list(keys))
        return original_delete(client, bucket, keys)

    monkeypatch.setattr(ingest_module, "_delete_objects_robust", _tracking_delete)

    events = _drain(ingest(source=src, delete_after=True))

    # Ingest still completes successfully — no crash from the broken submit.
    done = next(e for e in events if e["type"] == "done")
    assert done["rows_inserted"] == 1, f"ingest did not complete cleanly under broken executor; events={events!r}"

    # Critical: the inline fallback fired AND covered the key we ingested.
    # Without this, the file would stay in FOS forever as an orphan.
    assert inline_delete_calls, (
        "broken executor.submit triggered, but no inline _delete_objects_robust "
        "call followed — the ingested .gz would leak as a permanent FOS orphan."
    )
    assert any(key in batch for batch in inline_delete_calls), (
        f"inline delete fired but the ingested key {key!r} was not in any batch; got {inline_delete_calls!r}"
    )

    # And the row IS in ingested_files (the inline fallback runs AFTER
    # insert_ingested_files; the contract is that we never leave a file
    # marked-ingested-but-not-deleted on the FOS side).
    ingested = metadata_db.get_ingested_filenames(src["name"])
    assert any(key in name for name in ingested), (
        f"file marked-ingested-AND-deleted contract broken; ingested={ingested!r}"
    )


# ── combined failure modes in a single ingest tick (audit follow-up) ────────


def test_combined_failure_modes_in_single_tick(ingest_local_env, monkeypatch):
    """One ingest tick where THREE distinct failure modes hit different files:

      - file_a: present and healthy (ingests cleanly)
      - file_b: vanishes between LIST and GET (S3 deletion race / NoSuchKey)
      - file_c: present but corrupt gzip (DuckDB read raises)

    The contract: every healthy file lands in the buffer + metadata; every
    failed file is filtered out of ingested_files; the tick reports success.
    Each individual failure mode is exercised by its sibling test; this
    pins that they COMPOSE — a real bad day might see all three at once,
    and the recovery paths must not interact poorly (e.g. one path's
    cleanup must not retry a different path's file).
    """
    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]

    good_key = "raw/2026-06-02/09/2026-06-02T09-00-00.good.gz"
    gone_key = "raw/2026-06-02/09/2026-06-02T09-05-00.gone.gz"
    bad_key = "raw/2026-06-02/09/2026-06-02T09-10-00.bad.gz"

    _seed_local(log_dir, good_key, "2026-06-02T09:00:00Z")
    # gone_key intentionally NOT created — get_object raises NoSuchKey.
    # bad_key: present on disk but contents are invalid gzip → DuckDB reader fails.
    bad_path = log_dir / bad_key
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    bad_path.write_bytes(b"not a gzip file\x00\x01\x02")

    def _selective_get(Bucket, Key):  # noqa: N803
        if Key == gone_key:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "key vanished"}}, "GetObject"
            )
        return None  # fall through to default loader

    _install_mock_fos(monkeypatch, log_dir, keys=[good_key, gone_key, bad_key], get_object_handler=_selective_get)

    events = _drain(ingest(source=src))

    # Tick reports done (no top-level abort despite two failures).
    done = next(e for e in events if e["type"] == "done")
    assert done["rows_inserted"] == 1, (
        f"expected 1 healthy row to land; got rows_inserted={done['rows_inserted']!r}; events={events!r}"
    )

    from backend.core import metadata as metadata_db

    ingested = metadata_db.get_ingested_filenames(src["name"])
    # Only the good file made it.
    assert any(good_key in n for n in ingested), f"good file missing from ingested_files: {ingested!r}"
    assert not any(gone_key in n for n in ingested), f"vanished file leaked into ingested_files: {ingested!r}"
    assert not any(bad_key in n for n in ingested), f"corrupt file leaked into ingested_files: {ingested!r}"

    # No orphan in_flight — the failures were detected BEFORE the
    # mark-ingested step, not after. A leaked in_flight here would
    # accumulate across ticks.
    assert metadata_db.list_in_flight(src["name"]) == []


def test_fully_filtered_file_is_recorded_as_zero_row_marker(ingest_local_env, monkeypatch):
    """DATA-SAFETY producer side: a file whose rows are ALL excluded by the
    time-range WHERE filter (here, row timestamp past end_time, but the filename
    still passes the ±1h discovery pre-filter) writes NO buffer yet is still
    ledgered to suppress re-LIST. It must be recorded with row_count == 0 so the
    stranded-delete reconcile recognises it as a no-data marker and never deletes
    its raw .gz (the only copy of that data)."""
    from backend.core import metadata as metadata_db

    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]

    key = "raw/2026-06-01/10/2026-06-01T10-00-00.svc.gz"
    # Row timestamp is 10:00:30 — strictly AFTER the end_time gate below, so every
    # row is filtered out (valid_rows == 0), but the filename dt (10:00:00) is
    # within end_time + 1h so the file is NOT skipped at discovery.
    _seed_local(log_dir, key, "2026-06-01T10:00:30Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[key])

    events = _drain(
        ingest(
            source=src,
            delete_after=False,  # keep the raw so this is a real "marker" strand
            start_time="2026-06-01T09:00:00Z",
            end_time="2026-06-01T10:00:00Z",
        )
    )

    done = next(e for e in events if e["type"] == "done")
    assert done["rows_inserted"] == 0

    files = metadata_db.list_ingested_files(src["name"])
    marker = next((f for f in files if key in f["file_name"]), None)
    assert marker is not None, f"fully-filtered file should still be ledgered; got {files!r}"
    assert marker["row_count"] == 0, "a no-data marker must be recorded with row_count 0"

    # And the reconcile must never classify it reclaimable — even with a wide-open
    # (far-past) durability epoch, a row_count==0 marker is excluded.
    assert (
        metadata_db.get_reclaimable_strand_filenames(src["name"], {marker["file_name"]}, "2000-01-01 00:00:00") == set()
    )


def test_final_done_folds_strand_reclaim_with_new_file_deletes(ingest_local_env, monkeypatch):
    """End-to-end final-fold: a pre-existing durable strand (row_count>0, raw still
    in the bucket) is reclaimed by the reconcile while a brand-new file is ingested
    and deleted by the per-chunk path. The success done event's deleted_files must
    be the SUM of both, and the message must note the reclaim."""
    from backend.core import metadata as metadata_db

    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]
    bucket = src["bucket"]

    strand_key = "raw/2026-06-01/10/2026-06-01T10-00-00.strand.gz"
    new_key = "raw/2026-06-01/11/2026-06-01T11-00-00.fresh.gz"

    # Pre-seed the ledger with the strand as DURABLE (row_count>0) — i.e. a genuine
    # interrupted-delete, NOT a marker. Its raw is still listed in the bucket.
    metadata_db.insert_ingested_files(src["name"], [(f"s3://{bucket}/{strand_key}", 100, 5000)])
    # The new file has real, in-range rows so it actually ingests + per-chunk deletes.
    _seed_local(log_dir, new_key, "2026-06-01T11:00:00Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[strand_key, new_key])

    # The seeded strand is ingested at "now"; open the durability epoch so it is
    # treated as a reclaimable post-fix row.
    monkeypatch.setattr("backend.core.ingest._RECONCILE_LEDGER_EPOCH", "2000-01-01 00:00:00")
    events = _drain(ingest(source=src, delete_after=True))

    done = next(e for e in events if e["type"] == "done")
    assert done["new_files"] == 1, f"only the fresh file is new; events={events!r}"
    assert done["rows_inserted"] == 1
    # 1 strand reclaimed + 1 new file deleted by the per-chunk path = 2.
    assert done["deleted_files"] == 2, f"final-fold must sum reclaim + per-chunk deletes; got {done!r}"
    assert "reclaim" in done["message"].lower()


def test_pre_epoch_durable_strand_is_not_reclaimed(ingest_local_env, monkeypatch):
    """DATA-SAFETY: a durable strand whose ledger row predates the durability epoch
    is NOT reclaimed — its row_count can't be trusted (pre-fix rows recorded
    PRE-filter counts), so the reconcile leaves it for the 1-day ledger trim rather
    than risk deleting a legacy no-data marker."""
    from backend.core import metadata as metadata_db

    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]
    bucket = src["bucket"]
    strand_key = "raw/2026-06-01/10/2026-06-01T10-00-00.legacy.gz"

    metadata_db.insert_ingested_files(src["name"], [(f"s3://{bucket}/{strand_key}", 100, 5000)])
    _install_mock_fos(monkeypatch, log_dir, keys=[strand_key])

    # Epoch is in the far future relative to the seeded "now" row → pre-epoch.
    monkeypatch.setattr("backend.core.ingest._RECONCILE_LEDGER_EPOCH", "2999-01-01 00:00:00")
    events = _drain(ingest(source=src, delete_after=True))

    done = next(e for e in events if e["type"] == "done")
    assert done["deleted_files"] == 0, "a pre-epoch strand must not be reclaimed"
