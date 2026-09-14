# v3 Raw Log Layout Design

## Decision

Version 3 uses separate, explicit FOS roots for request and RUM logs:

```text
raw/request/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/analytics_log_*.json.gz
raw/rum/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/rum_beacon_*.json.gz
```

The bucket is dedicated to this project. v3 has no configurable object-key
prefix; these are the only raw roots.

The legacy v2 roots are not supported by v3:

```text
raw/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/analytics_log_*.json.gz
rum/raw/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/rum_beacon_*.json.gz
```

The RUM SDK asset remains under `rum/` and is not part of the raw
RUM log tree.

## Deployment contract

This is a breaking storage-layout change. A service configured for v2 must be
torn down and reprovisioned as v3 before it can use the v3 application. The
application must not silently reuse v2 logging endpoints or scan v2 folders.
Provisioning and startup validation should fail clearly when a service has not
been migrated to the v3 layout.

The v3 cutover sequence is:

1. Stop and tear down the v2 service logging endpoints and local ingest state.
2. Remove or archive any v2 raw objects according to the operator's teardown
   choice.
3. Provision the v3 request and RUM logging endpoints.
4. Deploy the v3 application and workers.
5. Verify the new endpoint paths, request/RUM discovery, and dashboard freshness.

## Architecture

`backend/provision/log_paths.py` remains the single source of truth. It will
expose explicit request and RUM path helpers and minute-prefix helpers for the
new roots. No caller may construct a parent `raw/` prefix and infer stream
ownership from object names.

Request ingestion will list only `raw/request/`. RUM ingestion will list only
`raw/rum/`. The two pipelines remain separate in sync mode and Celery ledger
mode. Exact object keys stored in ingest metadata remain the deletion contract,
so retention and post-commit deletion do not need to rediscover paths.

All producers and operational consumers must use the new helpers:

- Fastly logging endpoint provisioning and reconciliation.
- Synthetic request and RUM load generators.
- Sync and Celery discovery.
- RUM cleanup and retention.
- Usage accounting and diagnostics.
- Reset, teardown, and orphan cleanup.

DuckLake data, local buffers, and service metadata do not change their
locations. The raw key is retained only as provenance for ingested rows.

## Error handling and safety

Discovery must never scan the parent `raw/` root because that could route RUM
objects through the request parser. RUM discovery must never fall back to the
request root. Prefix helpers should return normalized keys without leading
slashes for S3/FOS APIs.

Provisioning must not update an existing v2 endpoint in place. It should
require the explicit v2 teardown/reprovision flow and report the required
action. Reset and teardown operations must target the explicit request and RUM
roots so one stream cannot delete the other accidentally.

## Testing and verification

Tests will cover:

- Exact request and RUM endpoint path templates.
- Exact fixed minute LIST prefixes.
- Request discovery from `raw/request/`.
- RUM discovery from `raw/rum/`.
- RUM exclusion from request discovery.
- Request and RUM retention/reset isolation.
- Provisioning refusal for an unconverted v2 service.
- Synthetic request rows containing dashboard-required client IPs.

The deployed test service will be verified by listing the new roots, uploading
paired request/RUM files, observing separate ledger commits, and confirming
request and RUM freshness independently in the dashboard header.
