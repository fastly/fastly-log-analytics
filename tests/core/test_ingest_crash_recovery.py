"""Crash-injection tests for the FOS ingest half (Deliverable 2, part 1).

``tests/core/test_ingest_in_flight.py`` already pins ``_recover_in_flight``
in isolation against *hand-set* in_flight rows. What was missing — and what
the 2026-06 durability work actually has to guarantee — is that a crash
*inside the real ``ingest()`` generator*, between each adjacent pair of
durability checkpoints, recovers correctly on the next tick with:

  * no double-count in ``ingested_files`` / ``ingested_files_summary``,
  * no orphaned (lost) raw ``.gz`` in FOS,
  * no duplicate buffer parquet (deterministic SHA-256 naming).

The four checkpoints in ``backend/core/ingest.py`` (per chunk, ~L876):

    1. record_in_flight        ─┐ crash here ⇒ buffer never written
    2. write_to_buffer          ├ crash here ⇒ buffer written, files unrecorded
    3. insert_ingested_files    ├ crash here ⇒ files recorded, in_flight dangling
    4. clear_in_flight         ─┘ crash here ⇒ in_flight dangling (already durable)

Each test crashes the real generator at one boundary, asserts the
intermediate on-disk / SQLite state, then runs a clean ``ingest()`` tick
(the "restart") and asserts the file lands exactly once.

The harness mirrors ``tests/test_e2e_pipeline.py::test_full_pipeline_including_raw_gzip_ingest``
— real ``ingest()`` + real PyIceberg against a local-FS warehouse + moto S3.
"""

from __future__ import annotations

import gzip
import io
import json
import os
from datetime import UTC, datetime, timedelta

import pytest

from backend.core import iceberg as ice
from backend.core import ingest as ing
from backend.core import metadata as metadata_db

# ── Harness ─────────────────────────────────────────────────────────────────


@pytest.fixture
def ingest_env(s3_mock, fos_source, tmp_path, monkeypatch):
    """Wire ``ingest()`` to moto S3 + a local-FS Iceberg warehouse.

    Yields a dict with the moto client, the source dict, and the warehouse /
    cache paths so a test can re-seed and re-run ``ingest()`` across a
    simulated restart.
    """
    cache_path = str(tmp_path / "cache")
    warehouse_path = str(tmp_path / "warehouse")
    os.makedirs(cache_path, exist_ok=True)
    os.makedirs(warehouse_path, exist_ok=True)

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.core.iceberg._warehouse_uri", lambda _src: f"file://{warehouse_path}")
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a, **kw: None)
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    # ingest.py holds its own module-level reference to _get_fos_client; wrap
    # the moto client to swallow the production-only ``caller_hint`` kwarg.
    class _Shim:
        def __init__(self, client):
            self._client = client

        def get_paginator(self, op, caller_hint=None):
            return self._client.get_paginator(op)

        def __getattr__(self, name):
            return getattr(self._client, name)

    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda _src: _Shim(s3_mock))

    for cache in (ice._catalog_cache, ice._snapshot_files_cache, ice._table_object_cache):
        cache.clear()
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    ice.init_iceberg_table(fos_source)
    return {
        "s3": s3_mock,
        "src": fos_source,
        "warehouse": warehouse_path,
        "cache": cache_path,
        "bucket": fos_source["bucket"],
    }


def _seed_gz(s3, bucket: str, key: str, rows: list[dict]) -> None:
    body = io.BytesIO()
    with gzip.GzipFile(fileobj=body, mode="wb") as gz:
        gz.write(("\n".join(json.dumps(r) for r in rows) + "\n").encode())
    s3.put_object(Bucket=bucket, Key=key, Body=body.getvalue())


def _one_log_file(s3, bucket: str, n_rows: int = 4) -> tuple[str, int]:
    """Seed a single Fastly-shaped .gz log file; return (key, n_rows)."""
    base = datetime.now(UTC) - timedelta(hours=2)
    key = "raw/2026-05-20/10/2026-05-20T10-00-00.svc.gz"
    rows = [
        {
            "timestamp": (base + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%S+0000"),
            "ip": f"10.0.0.{i}",
            "status": 200 if i % 2 == 0 else 404,
            "url": f"/path/{i}",
            "method": "GET",
            "cache": "HIT",
            "resp_bytes": 1000 + i,
        }
        for i in range(n_rows)
    ]
    _seed_gz(s3, bucket, key, rows)
    return key, n_rows


def _drain(gen) -> list[dict]:
    return list(gen)


def _ingested_names(sid: str) -> set[str]:
    return metadata_db.get_ingested_filenames(sid)


def _summary(sid: str) -> dict:
    return metadata_db.get_ingested_files_status_summary(sid)


class _RaiseOnce:
    """Wrap a callable so the first call raises, later calls delegate.

    Lets us crash the FIRST ingest tick at a precise checkpoint while
    leaving the same function healthy for the recovery tick.
    """

    def __init__(self, real, exc: Exception):
        self._real = real
        self._exc = exc
        self.calls = 0

    def __call__(self, *a, **kw):
        self.calls += 1
        if self.calls == 1:
            raise self._exc
        return self._real(*a, **kw)


# ── Checkpoint 1→2: crash before write_to_buffer ────────────────────────────


def test_crash_before_write_to_buffer_recovers_clean(ingest_env, monkeypatch):
    """record_in_flight ran, write_to_buffer dies. The buffer never landed,
    so recovery must DROP the dangling in_flight row, leave ingested_files
    empty, and the raw .gz must still be in FOS to re-ingest cleanly."""
    s3, src, sid = ingest_env["s3"], ingest_env["src"], ingest_env["src"]["name"]
    key, n = _one_log_file(s3, ingest_env["bucket"])

    # _RaiseOnce: the FIRST write_to_buffer (this tick) crashes; the restart
    # tick's call delegates to the real implementation. No monkeypatch.undo()
    # here — the autouse metadata-isolation fixture shares this monkeypatch
    # instance and undo() would repoint the metadata DB at the real DATA_DIR.
    monkeypatch.setattr(
        "backend.core.iceberg.write_to_buffer",
        _RaiseOnce(ice.write_to_buffer, RuntimeError("crash: disk full mid write_to_buffer")),
    )

    with pytest.raises(RuntimeError, match="write_to_buffer"):
        _drain(ing.ingest(source=src, delete_after=True))

    # Intermediate state: in_flight dangling, nothing recorded, buffer absent,
    # raw file NOT deleted (delete happens after the crash point).
    assert _ingested_names(sid) == set()
    assert len(metadata_db.list_in_flight(sid)) == 1
    assert ice.buffer_files(src) == []
    assert "Contents" in s3.list_objects_v2(Bucket=ingest_env["bucket"], Prefix="raw/"), "raw .gz orphaned/lost"

    # Restart: the recovery sweep drops the dangling row, then the file
    # re-LISTs and ingests cleanly (write_to_buffer now delegates).
    done = next(e for e in ing.ingest(source=src, delete_after=True) if e["type"] == "done")
    assert done["rows_inserted"] == n
    # Exactly one ingested-files row for the file; in_flight drained.
    assert len([x for x in _ingested_names(sid) if x.endswith(key)]) == 1
    assert metadata_db.list_in_flight(sid) == []
    assert _summary(sid)["total_rows"] == n  # no double-count in the rollup


# ── Checkpoint 2→3: crash before insert_ingested_files ───────────────────────


def test_crash_before_insert_ingested_files_promotes_buffer(ingest_env, monkeypatch):
    """write_to_buffer succeeded, insert_ingested_files dies. The buffer is on
    disk but its files are unrecorded. Recovery must PROMOTE the buffer's
    files into ingested_files (no re-ingest, no duplicate buffer) and the
    eventual commit must write the rows exactly once."""
    s3, src, sid = ingest_env["s3"], ingest_env["src"], ingest_env["src"]["name"]
    key, n = _one_log_file(s3, ingest_env["bucket"])

    raiser = _RaiseOnce(metadata_db.insert_ingested_files, RuntimeError("crash: DB locked before insert"))
    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", raiser)

    with pytest.raises(RuntimeError, match="before insert"):
        _drain(ing.ingest(source=src, delete_after=True))

    # Buffer present, files unrecorded, in_flight dangling.
    assert len(ice.buffer_files(src)) == 1
    assert _ingested_names(sid) == set()
    assert len(metadata_db.list_in_flight(sid)) == 1
    buf_before = set(os.path.basename(p) for p in ice.buffer_files(src))

    # Restart: _recover_in_flight (insert healthy on the 2nd call) promotes.
    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", raiser._real)
    done = next(e for e in ing.ingest(source=src, delete_after=True) if e["type"] == "done")

    # The recovered file must NOT be re-listed/re-ingested (it's now recorded),
    # so the restart tick ingests 0 new rows and produces no second buffer.
    assert done["rows_inserted"] == 0, "recovered file was re-ingested — double-count risk"
    assert len([x for x in _ingested_names(sid) if x.endswith(key)]) == 1
    assert metadata_db.list_in_flight(sid) == []
    assert set(os.path.basename(p) for p in ice.buffer_files(src)) == buf_before, "duplicate buffer created"
    assert _summary(sid)["total_rows"] == n

    # Commit the promoted buffer → rows land in Iceberg exactly once.
    res = ice.commit_buffer(src)
    assert res["rows_committed"] == n


# ── Checkpoint 3→4: crash before clear_in_flight ─────────────────────────────


def test_crash_before_clear_in_flight_is_idempotent(ingest_env, monkeypatch):
    """insert_ingested_files succeeded (file + summary durable), clear_in_flight
    dies. Recovery re-promotes: it re-inserts the SAME file (idempotent upsert)
    and clears. The critical invariant is the ``ingested_files_summary`` rollup
    must NOT double-count the re-insert (delta must be 0)."""
    s3, src, sid = ingest_env["s3"], ingest_env["src"], ingest_env["src"]["name"]
    key, n = _one_log_file(s3, ingest_env["bucket"])

    raiser = _RaiseOnce(metadata_db.clear_in_flight, RuntimeError("crash: DB locked before clear"))
    monkeypatch.setattr("backend.core.metadata.clear_in_flight", raiser)

    with pytest.raises(RuntimeError, match="before clear"):
        _drain(ing.ingest(source=src, delete_after=True))

    # File recorded once, summary == n, in_flight dangling, buffer present.
    assert len([x for x in _ingested_names(sid) if x.endswith(key)]) == 1
    assert _summary(sid)["total_rows"] == n
    assert _summary(sid)["file_count"] == 1
    assert len(metadata_db.list_in_flight(sid)) == 1
    assert len(ice.buffer_files(src)) == 1

    # Restart: recovery re-inserts (idempotent) + clears.
    monkeypatch.setattr("backend.core.metadata.clear_in_flight", raiser._real)
    next(e for e in ing.ingest(source=src, delete_after=True) if e["type"] == "done")

    assert metadata_db.list_in_flight(sid) == []
    assert _summary(sid)["total_rows"] == n, "summary rollup double-counted the re-insert"
    assert _summary(sid)["file_count"] == 1, "summary file_count double-counted the re-insert"
    res = ice.commit_buffer(src)
    assert res["rows_committed"] == n


# ── SHA-256 deterministic buffer name: "same file twice" after a crash ───────


def test_sha256_name_prevents_duplicate_buffer_if_recovery_skipped(ingest_env, monkeypatch):
    """Belt-and-suspenders: even if the in_flight recovery sweep is itself
    lost (DB blip), a raw file LISTed twice across a crash must not produce
    two buffer parquets — the SHA-256 chunk name makes the second write
    OVERWRITE the first, so commit can't double-append.

    Setup: crash before insert (file stays unrecorded, raw stays in FOS),
    then restart with the recovery sweep neutralised — the file is re-LISTed
    and re-ingested, hitting the same deterministic buffer name.
    """
    s3, src, sid = ingest_env["s3"], ingest_env["src"], ingest_env["src"]["name"]
    key, n = _one_log_file(s3, ingest_env["bucket"])

    raiser = _RaiseOnce(metadata_db.insert_ingested_files, RuntimeError("crash before insert"))
    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", raiser)
    with pytest.raises(RuntimeError):
        _drain(ing.ingest(source=src, delete_after=False))

    bufs_after_crash = ice.buffer_files(src)
    assert len(bufs_after_crash) == 1
    crashed_buf_name = os.path.basename(bufs_after_crash[0])

    # Restart but DISABLE the recovery sweep (simulate the sweep itself
    # failing) AND drop the dangling in_flight row so the file looks brand
    # new again — the worst case the SHA naming must survive.
    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", raiser._real)
    monkeypatch.setattr(
        "backend.core.ingest._recover_in_flight", lambda _src: {"promoted": 0, "dropped": 0, "rows_recovered": 0}
    )
    metadata_db.clear_in_flight(sid, crashed_buf_name)

    done = next(e for e in ing.ingest(source=src, delete_after=False) if e["type"] == "done")
    assert done["rows_inserted"] == n  # re-ingested the same file

    # The deterministic name means the re-ingest OVERWROTE the same buffer —
    # exactly one buffer file, so commit appends the rows once, not twice.
    bufs = ice.buffer_files(src)
    assert len(bufs) == 1, f"SHA-256 naming failed to dedupe — {len(bufs)} buffers: {bufs}"
    assert os.path.basename(bufs[0]) == crashed_buf_name
    res = ice.commit_buffer(src)
    assert res["rows_committed"] == n, "double-commit despite deterministic buffer name"
