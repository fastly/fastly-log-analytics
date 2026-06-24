import gzip
import json

from backend.core.ingest import ingest


def _drain_ingest(gen):
    events = []
    for e in gen:
        events.append(e)
    return events


def test_ingest_skips_corrupted_gzip_files(fos_source, in_memory_duckdb, monkeypatch, tmp_path):
    """Verify that ingest handles corrupted .gz files by skipping them via the isolation loop.

    Pinned because a single malformed file (e.g. truncated upload) must NOT
    abort the entire batch or crash the cron scheduler.
    """
    # 1. Setup local mock logs directory
    log_dir = tmp_path / "mock_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Valid file: standard gzip
    valid_data = {"timestamp": "2026-05-18T10:00:00Z", "status": 200, "url": "/ok"}
    valid_file = log_dir / "raw/2026-05-18/10/2026-05-18T10-00-00.v.gz"
    valid_file.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(valid_file, "wt") as f:
        f.write(json.dumps(valid_data) + "\n")

    # Corrupted file: not a gzip file, but ends in .gz
    corrupt_file = log_dir / "raw/2026-05-18/10/2026-05-18T10-05-00.c.gz"
    corrupt_file.write_bytes(b"This is definitely not a gzip file")

    # 2. Redirect cache and disable FOS config
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _: str(tmp_path / "cache"))
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a: None)
    monkeypatch.setattr("time.sleep", lambda *a: None)

    # 3. Mock DuckDB memory connection
    monkeypatch.setattr("backend.core.duckdb.get_memory_connection", lambda _: in_memory_duckdb)

    # 4. Mock FOS client paginator to "discover" these files
    class MockPaginator:
        def paginate(self, **kwargs):
            return [
                {
                    "Contents": [
                        {"Key": "raw/2026-05-18/10/2026-05-18T10-00-00.v.gz", "Size": 100},
                        {"Key": "raw/2026-05-18/10/2026-05-18T10-05-00.c.gz", "Size": 100},
                    ]
                }
            ]

    class MockFos:
        def get_paginator(self, *args, **kwargs):
            return MockPaginator()

        def get_object(self, Bucket, Key):
            # Ingest now pre-downloads each chunk via boto3.get_object before
            # passing local paths to DuckDB. Serve from the local mock_logs
            # dir so tests don't need a real S3 endpoint.
            import io

            with open(log_dir / Key, "rb") as fh:
                return {"Body": io.BytesIO(fh.read())}

        def delete_objects(self, **kwargs):
            return {}

    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda *a: MockFos())

    # 5. Route ingest through local filesystem.
    # Production ingest hands s3://bucket/... paths to DuckDB and lets the
    # `fos_proxy` SECRET route them through the telemetry proxy → CDN. In
    # tests we have no proxy, so wrap the in-memory DuckDB connection's
    # `execute` to rewrite s3://test-bucket/... → file://localdir/... before
    # the SQL hits httpfs. This is the test-only equivalent of the
    # production-removed `s3://bucket` → `cdn_url` rewrite.
    test_src = {**fos_source}
    bucket_prefix = f"s3://{fos_source['bucket']}/"
    file_prefix = "file://" + str(log_dir.absolute()) + "/"

    class _S3RewritingConn:
        # DuckDBPyConnection's execute is C-level read-only, so wrap rather
        # than monkeypatch. Used to substitute the production-removed
        # cdn_url rewrite at the SQL boundary.
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *args, **kwargs):
            if isinstance(sql, str) and bucket_prefix in sql:
                sql = sql.replace(bucket_prefix, file_prefix)
            return self._conn.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    import backend.core.duckdb as my_duckdb

    monkeypatch.setattr(my_duckdb, "get_memory_connection", lambda src: _S3RewritingConn(in_memory_duckdb))

    # 6. Run ingest
    # Small chunk size to force batch failure and trigger isolation
    monkeypatch.setattr("backend.core.duckdb.INGEST_CHUNK_SIZE", 10)

    events = _drain_ingest(ingest(source=test_src))

    # 7. Assertions
    done = next(e for e in events if e["type"] == "done")

    # new_files counts attempted/processed files (both)
    assert done["new_files"] == 2
    # rows_inserted should only reflect the valid file
    assert done["rows_inserted"] == 1

    # Check metadata_db record - only the valid file should be tracked
    from backend.core import metadata as metadata_db

    ingested = metadata_db.get_ingested_filenames(test_src["name"])

    # The corrupted file failed isolation and should NOT be in the metadata DB
    assert len(ingested) == 1
    valid_path = f"s3://{fos_source['bucket']}/raw/2026-05-18/10/2026-05-18T10-00-00.v.gz"
    assert any(valid_path in f for f in ingested)

    # Verify DuckDB buffer has the data
    from backend.core import iceberg

    buffer_files = iceberg.buffer_files(test_src)
    assert len(buffer_files) >= 1
