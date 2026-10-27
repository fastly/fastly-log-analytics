---
title: SET unsafe_enable_version_guessing triggers iceberg extension autoload on read-only FS
author: Drew
date: 2026-10-27
branch: release/v3.0.0-beta4
id: l-2118556c
scope: repo
status: active
src: agent
touches:
  - "backend/core/duckdb.py#get_connection"
  - "backend/core/storage/backup.py#_push_worker"
---

Removing leftover SET unsafe_enable_version_guessing=true; from backend/core/duckdb.py:get_connection prevents DuckDB 1.5.4 from attempting to autoload/install the iceberg extension into /home/app/.duckdb/extensions on read-only High-Scale Kubernetes container filesystems. Also log background cloud-push retries without exc_info=True in backend/core/storage/backup.py:_push_worker so transient S3 read timeouts do not emit Rich Traceback blocks into container logs.
