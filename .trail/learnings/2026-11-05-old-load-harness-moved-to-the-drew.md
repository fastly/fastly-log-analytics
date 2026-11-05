---
title: Old load harness moved to the shared load-testing tool
author: Drew
date: 2026-11-05
branch: feat/fdlt-harness
id: l-55a1d282
scope: repo
status: active
src: agent
touches:
  - "app/tools/deploy_test_all.py#step_4_deploy_check"
  - app/Makefile
  - "app/src/telemetry/usage.rs#mark_harness"
---

FLP's own load sender and verifier (app/tools/loadtest, app/tools/loadharness, capacity_campaign.py and their tests) were removed. Load tests now run in ~/Projects/fastly-log-analytics-load-testing (the lt CLI, flp adapter), configured by fastly-lt.toml at the repo root: make -C app deploy-check for one short stage, make -C app loadtest-campaign CAMPAIGN=<name> for the ladder. deploy_test_all.py Step 4 shells out to lt deploy-check and maps its campaign.json stage onto the old report keys; schema validity is reported as None because the shared tool does not check FLP's Parquet schema. The S3 signing and execution-log credential helpers live in the shared tool's lt/adapters/flp_s3.py, so do not re-add them here. Harness attribution stays app-side: X-FLP-Harness labels api_telemetry details_json, fos_usage rows (caller prefix harness: plus a harness column) and the folded /api/file beacon row. The fastly-lt.toml token source is FASTLY_API_KEY in ~/Projects/fastly-log-analytics/.env, the same token as the Elevation service.
