# ClickHouse Admin (`/admin/clickhouse`)

Status and bounded replay controls for the ClickHouse diagnostic index. The link appears on the Admin page only when a service is high-scale. The index is the explicit pinned-snapshot export described in [ADR-20](../../adr/20-clickhouse-serving-plane.md) (a rejected serving prototype kept as a diagnostic and replay tool); the high-scale serving path is in [ADR-21](../../adr/21-high-scale-serving-architecture.md).

Source: [`frontend/app/admin/clickhouse/`](../../../frontend/app/admin/clickhouse/) (`page.tsx`, `loading.tsx`, `_sections/ClickHouseAdminClient.tsx`, `ClickHouseVitalsAndGeneration.tsx`, `ClickHouseReplayControlPanel.tsx`).

## URL parameters

`service` (optional; falls back to the active service).

## UI

- Toolbar with the service, a health badge and Refresh.
- Cluster & Publication Vitals: engine enabled state, schema version, publication counts (`pending`, `claimed`, `failed`, `published`), oldest pending age, last published time, and expired dataset count.
- Active generation card: generation, dataset ID, coverage window, expiry, whether the target matches, coverage age.
- Bounded Artifact Replay Control Panel: choose Dataset Rebuild (`dataset_id`) or Generation Resume (`generation`), set the artifact limit, and keep Dry Run Preview checked or uncheck it to run for real. When the engine is disabled the controls are disabled with an explanation.
- A 503 status response is parsed and shown as `unavailable` rather than a generic error.

## Roles

Admin only (`require_admin`; `/api/admin/` is analyst-blocked).

## Backend

Routes in [`backend/routers/admin/clickhouse.py`](../../../backend/routers/admin/clickhouse.py); models `ClickHouseStatusResponse`, `ClickHouseReplayRequest`, `ClickHouseReplayResponse` in [`backend/models/admin/high_scale.py`](../../../backend/models/admin/high_scale.py); ClickHouse client and manifest code in [`backend/core/clickhouse/`](../../../backend/core/clickhouse/).

- `GET /api/admin/clickhouse/status?service_id=...` (`ClickHouseStatusResponse`): `enabled`, `health` (`ok`, `disabled`, `unavailable`), `schema_version`, `publication_counts`, `oldest_pending_age_seconds`, `last_published_at`, `active_generation`, `expired_dataset_count`. Without a configured ClickHouse client it returns 200 with `enabled: false, health: "disabled"`; a connection failure or schema-version mismatch returns 503 with `health: "unavailable"`. Missing `service_id` is 400.
- `POST /api/admin/clickhouse/replay` (`ClickHouseReplayRequest`, `extra="forbid"`): `service_id`, exactly one of `dataset_id` or `generation`, `dry_run` (default true), `limit` (default 100). Over 100 artifacts is `422 invalid_replay_limit`; a missing or doubled reference is 422; a disabled engine is `409 clickhouse_disabled`; an unknown reference is `404 replay_not_found`; an expired, incompatible or over-cap reference is `409 replay_ineligible`; execution failure is a sanitized `503 replay_unavailable`. A dry run only validates retained metadata references (no FOS access, no audit rows, no manifest changes) and returns `planned_artifacts`. A real run loads sealed artifacts from FOS and publishes them, and records `requested`, `completed` or `failed` events in `audit_logs`. Responses never include artifact URLs or credentials.

The HTTP cap is 100 artifacts per request **and per dataset** (activation verifies the whole dataset) and the work is synchronous; larger datasets use the CLI (`python -m scripts.clickhouse_replay`). These endpoints are diagnostic index/replay tooling, not dashboard serving (dashboard endpoints call the DuckLake repository in Standard mode regardless of `CLICKHOUSE_ENABLED`, and there is no separate serving flag). Key invariants:
- Use real psycopg pool transactions, not a wrapper whose `commit()` is a no-op.
- All reads on the fact tables require `FINAL`; only complete, content-verified generations activate (rebuilds allocate a new generation and load every artifact).
- Disabled/unavailable measurements are `null`, not zero; an expired active generation stays visible with `expired=true`.
- Replay requires a `read_write` service and uses only `FosArtifacts.load`, never export.
- Metrics: query IDs belong in logs, never metric labels; unknown lag/disk is not zero; enabled callbacks share a bounded authenticated disk probe and never run full generation readiness; replay CLI commands force-flush telemetry before exit.

## Tests

- E2E: `Page Spec: /admin/clickhouse` in [`frontend/e2e/pages/admin-overview.spec.ts`](../../../frontend/e2e/pages/admin-overview.spec.ts)
- Backend: [`tests/core/test_clickhouse.py`](../../../tests/core/test_clickhouse.py), [`tests/routers/test_admin_system_and_streams.py`](../../../tests/routers/test_admin_system_and_streams.py)
