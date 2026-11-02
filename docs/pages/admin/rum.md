# RUM Configuration (`/admin/rum`)

Admin page for a service's Real User Monitoring setup: enablement, collector settings, the pinned Faro Web SDK version and VCL drift. The analytics themselves are on [`/rum`](../rum.md).

Source: [`frontend/app/admin/rum/page.tsx`](../../../frontend/app/admin/rum/page.tsx) (a thin page with a Back to Admin link that renders `RumStatusPanel` from [`frontend/app/rum/_sections/RumStatusSection.tsx`](../../../frontend/app/rum/_sections/RumStatusSection.tsx)); dialogs in [`frontend/components/Rum/`](../../../frontend/components/Rum/) (`RumSettingsDialog.tsx`, `RumLifecycleDialogs.tsx`).

## UI

- Status card: enabled state and enable time, a VCL drift warning with a re-apply action when the deployed VCL no longer matches the expected one, refresh.
- Enable RUM and Disable RUM dialogs that stream provisioning progress.
- Settings dialog: capture toggles `capture_vitals`, `capture_performance`, `capture_errors`, `capture_events`, and a custom VCL condition.
- Faro SDK version card (current, latest, update available) with an upgrade action.

## Roles

Admin only. Enable, disable, settings, versions and upgrade are `require_admin` and are also blocked for analysts by `_ANALYST_BLOCKED_RUM_SUFFIXES` (`403 admin_only`). Analysts may call `GET /api/services/{id}/rum/status`, which returns only `{enabled, enabled_at}`.

## Backend

Routes in [`backend/routers/rum/provisioning.py`](../../../backend/routers/rum/provisioning.py), prefix `/api/services`; models in [`backend/models/analytics/rum.py`](../../../backend/models/analytics/rum.py) and [`backend/models/common/provision.py`](../../../backend/models/common/provision.py).

- `GET /api/services/{service_id}/rum/status` (`RumStatusResponse`): `enabled`, `enabled_at`, `deployed_vcl_sha`, `current_vcl_sha`, `vcl_drift`, `capture_*`, `custom_condition`.
- `POST .../rum/enable` and `POST .../rum/disable` (SSE): provision or tear down the Fastly VCL snippets and the self-hosted SDK bundle in the service's FOS bucket.
- `POST .../rum/settings` (`RumSettingsUpdateRequest`: the four `capture_*` booleans, `custom_condition`, `token`): updates config and redeploys the tracker; returns `skipped` when nothing changed.
- `GET .../rum/versions` (`RumVersionsResponse`: `available`, `current`, `latest`, `update_available`) and `POST .../rum/upgrade` (SSE, `version`, `token`): pin a different Faro SDK version; unknown versions are `400 unknown_faro_version`.

RUM ingest is driven by the `rum_sync` / `rum_commit` crons in standard mode and by the high-scale worker in high-scale mode (see [docs/cron](../../cron/README.md)); the tables and read contract are described in [rum.md](../rum.md).

## Tests

- E2E: `/admin/rum — Admin RUM Ingestion & Configuration Contract` in [`frontend/e2e/pages/admin-overview.spec.ts`](../../../frontend/e2e/pages/admin-overview.spec.ts)
- Backend: [`tests/routers/test_provision.py`](../../../tests/routers/test_provision.py), [`tests/cron/test_rum_sync.py`](../../../tests/cron/test_rum_sync.py)
