# Monkeypatch Inventory & Elimination TODO

This file catalogs every third-party class/function we monkeypatch at import
time so we can audit, justify, and eventually replace them with cleaner
abstractions.

## Status (v3.0)

**Zero active third-party monkeypatches remain.**

All five `s3fs` / `botocore` patches (`S3FileSystem.__init__`,
`S3FileSystem.set_session`, `S3FileSystem._cat_file`, `S3FileSystem._info`,
`S3FileSystem._open`) and the `ThreadPoolExecutor.submit` ContextVar patch
formerly in `backend/core/iceberg/fs.py` were retired in v3.0 alongside the
removal of `pyiceberg` and `s3fs` in favor of DuckDB's native DuckLake
extension.

## How to add an entry

When you add a new monkeypatch:

1. Add the patch with an inline comment block explaining **what telemetry
   incident** justified it (date + 1-2 sentence symptom).
2. Add an entry here with site/what/why/cleanup.
3. Cross-link from the patch site to the relevant section header in this
   file.

When you remove a monkeypatch: delete the entry, don't leave a tombstone.
