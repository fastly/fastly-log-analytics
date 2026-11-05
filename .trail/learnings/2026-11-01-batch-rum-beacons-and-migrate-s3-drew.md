---
title: Batch RUM beacons and migrate S3 log endpoints + ingestion from gzip to zstd
author: Drew
date: 2026-11-01
branch: v3.0.0-rc1
id: l-20f84aae
scope: repo
status: active
src: agent
touches:
  - backend/core/ingest/__init__.py
  - backend/core/ingest/discovery.py
  - backend/core/ingest/parser.py
  - backend/core/metadata/ingest_log.py
  - backend/core/rum_ingest.py
  - backend/high_scale/query.py
  - backend/provision/declarative/fastly_ops.py
  - backend/provision/declarative/models.py
  - backend/provision/rum_assets.py
  - backend/utils/terraform_gen.py
---

When migrating Fastly S3 logging endpoints from gzip to zstd, Fastly's API rejects compression_codec='zstd' if gzip_level > 0; _upsert_version_resource in backend/provision/declarative/fastly_ops.py must first PUT {'gzip_level': 0}, and fetch_logging_endpoints must populate compression_codec='gzip' when gzip_level > 0 so compute_diff detects drift. For decompression, zstandard.ZstdDecompressor().decompress() fails on streaming frames without a content-size header; always use ZstdDecompressor().stream_reader(...) and normalize .zstd local temp filenames to .zst so DuckDB read_json_auto auto-detects zstd.
