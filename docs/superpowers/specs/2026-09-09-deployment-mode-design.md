# Deployment Mode Configuration Design

## Decision

Expose one deployment topology variable:

```text
DEPLOYMENT_MODE=standard
DEPLOYMENT_MODE=high_throughput
```

`standard` maps to synchronous ingest and file-backed serving. `high_throughput`
maps to Celery ingest and durable serving against the shared Postgres-backed
DuckLake catalog.

`INGEST_MODE` and `SERVING_MODE` are removed entirely. Mode-specific code uses
`DEPLOYMENT_MODE` directly or shared predicates such as
`is_high_throughput_mode()` where a semantic branch is clearer.

## Configuration and validation

`backend.config` reads and normalizes `DEPLOYMENT_MODE`, defaulting to
`standard`, and rejects unknown deployment modes during startup validation.

`high_throughput` retains the existing requirements for a Celery broker,
Postgres `DUCKLAKE_CATALOG`, and Postgres `METADATA_DSN`. `standard` retains
the existing single-node file-backed behavior.

All environment examples, Compose and Helm configuration, deployment tests,
and operator documentation use `DEPLOYMENT_MODE`. The old two-variable
configuration is removed from the documented and tested public interface.

## Scope

Update configuration, deployment manifests, chart defaults and validation,
backend tests, load-test environment checks, frontend backend-environment
fixtures, architecture and runbook documentation, and the repository agent
guide wherever the old public variables are described.

Rewrite mode-specific backend branches to use `DEPLOYMENT_MODE` or shared
predicates. Do not alter feature behavior or deployment topology beyond
removing the duplicate configuration variables.

## Verification

Tests must cover:

- `standard` selects synchronous ingest and file-backed serving.
- `high_throughput` selects Celery ingest and durable serving.
- Invalid deployment modes fail validation.
- Required durable dependencies remain enforced.
- Chart and environment fixtures emit the new variable and values.
