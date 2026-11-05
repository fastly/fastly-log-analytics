---
title: summary_bundle binary partial state and pre-downcast Cols keep vCPU under 50ms
author: Drew
date: 2026-11-02
branch: main
cost: { minutes: 92, tokens: 660720 }
id: l-cc98294d
scope: repo
status: active
src: agent
touches:
  - app/src/ingest/bundle.rs
  - app/src/ingest/summary.rs
  - app/tests/ingest.rs
---

Writing and reading summary_bundle partial= files as 73-column ZSTD Parquet cost 70-146ms of vCPU per execution on live 36-window hours. Encoding partial= with PARTIAL_MAGIC binary format (sparse HLL registers), pre-downcasting Cols once per RecordBatch, looking up &str keys before allocating Strings, and deferring the ~18ms final Parquet write when folding spent >= 20ms keeps every summary_bundle execution strictly under 50ms vCPU.
