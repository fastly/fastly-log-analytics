---
title: Propagate contextvars into ingest ThreadPoolExecutor workers and include ctx in _configure_fos secret fingerprint
author: Drew
date: 2026-10-28
branch: release/v3.0.0-beta4
id: l-844a4fda
scope: repo
status: active
src: agent
touches:
  - backend/core/duckdb.py
  - backend/core/ingest/discovery.py
  - backend/core/ingest/writer.py
  - backend/core/storage/catalog.py
  - backend/utils/telemetry/proxy_server.py
  - tests/utils/test_telemetry.py
---

In backend/core/ingest/discovery.py (_download_chunk_to_local, _delete_objects_robust_with_failures) and backend/core/ingest/writer.py (_delete_executor), ThreadPoolExecutor.submit does not inherit Python ContextVars (_PROCESS_CONTEXT). Without contextvars.copy_context().run, install_boto3_proxy_hook fell back to get_process_context_with_fallback() (_LATEST_PROCESS_CONTEXT), causing background raw .gz downloads to inherit concurrent api:* contexts and trigger false-positive '[telemetry-proxy] dashboard context hitting FOS' warnings. Additionally, _configure_fos in backend/core/duckdb.py must include ctx in _fos_secret_fingerprints so X-Telemetry-Context on SECRET fos_proxy refreshes when the process context changes.
