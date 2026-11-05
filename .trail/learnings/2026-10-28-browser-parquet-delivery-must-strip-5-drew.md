---
title: Browser Parquet delivery must strip 5 MiB S3 multipart padding and redact PII
author: Drew
date: 2026-10-28
branch: main
id: l-2c22cb76
scope: repo
status: active
src: agent
touches:
  - "app/src/query/aggregate.rs#compact_file"
  - app/src/pipeline/worker.rs
---

Stitched Parquet objects in FOS have non-final worker parts zero-padded to 5 MiB (5,242,880 bytes) in app/src/pipeline/worker.rs to satisfy S3 UploadPart minimum part sizes. Serving raw FOS Parquet objects directly to the browser would waste >95% bandwidth on zero padding and leak unredacted URLs, client_ip, and _extra. Instead, app/src/query/aggregate.rs#compact_file (panel=file) reads only the 17 safe columns via coalesced row-group range reads (source.bound_columns), redacts url query/path-tokens and client_ip (/16), and re-serializes a compact Snappy Parquet payload with immutable Cache-Control headers for browser Cache API + hyparquet.
