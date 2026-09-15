# Hourly Rollup Catch-up Design

## Goal

Prevent a backend restart from OOMing while rebuilding durable-serving rollup
coverage. The dashboard must become available immediately after startup, while
historical rollup coverage is repaired by the existing hourly self-heal job.

## Design

Durable-mode service initialization will stop running the 30-day
`backfill_missing_hour_bundles` pass synchronously in the startup worker. It
will leave rollup readiness unset and complete normal service initialization
without waiting for historical rollups.

The existing `rollup_hour_heal` cron remains responsible for the catch-up. Its
existing readiness gate will continue to use the deep lookback until coverage
is verified, then switch to the one-day steady-state lookback and mark the
service ready for durable rollup reads. The dashboard therefore uses the
existing safe raw-query path while coverage is incomplete rather than serving
partial rollups.

No data-plane, ClickHouse, Postgres, or API contract changes are included.
Rollup writes remain local-only and idempotent. A failed hourly pass records a
cron error and leaves readiness unset so a later pass can retry.

## Validation

1. Add startup tests proving durable initialization does not invoke the
   backfill and does not mark coverage ready.
2. Preserve and run the hourly-heal tests covering deep catch-up before
   readiness and one-day operation after readiness.
3. Deploy to `infra:dev-usc1`, restart the backend, and verify:
   - no OOM restart during startup;
   - `/api/health?deep=1` responds;
   - the dashboard returns populated data;
   - the hourly job can complete or explicitly record an error without
     corrupting serving state.
4. Re-measure dashboard latency after rollup readiness is established and
   continue high-scale qualification only after the restart path is stable.
