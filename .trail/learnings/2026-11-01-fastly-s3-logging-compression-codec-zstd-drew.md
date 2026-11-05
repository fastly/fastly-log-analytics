---
title: Fastly S3 logging compression_codec zstd vs gzip_level mutual exclusivity
author: Drew
date: 2026-11-01
branch: main
id: l-f61b54d7
scope: repo
status: active
src: agent
touches:
  - "app/src/logging/mod.rs#logging_endpoint_body"
  - "app/src/ingest/convert.rs#read_lines"
  - "app/src/ingest/mod.rs#window_seconds_for_period"
---

Fastly's S3 logging endpoint API rejects payloads that send both compression_codec and gzip_level simultaneously with HTTP 400 ('compression_codec and gzip_level cannot be set simultaneously'). In app/src/logging/mod.rs#logging_endpoint_body, when compression_codec is set to zstd (or snappy), omit gzip_level from the request payload; Fastly automatically resets gzip_level to 0. Conversely, when switching an existing zstd endpoint back to gzip, send compression_codec as empty string alongside gzip_level. In app/src/ingest/convert.rs#read_lines, raw log objects are auto-detected by the 4-byte zstd magic header [0x28, 0xb5, 0x2f, 0xfd] before falling back to MultiGzDecoder.
