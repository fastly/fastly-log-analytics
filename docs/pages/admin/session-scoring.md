# Session Scoring (`/admin/session-scoring`)

Admin console for edge session scoring: a 0 to 100 risk score per request that combines cookie and timing signals (L1) with a Laplace-smoothed direct route-transition anomaly matrix (L2, with anchor skip-grams disabled). The Rust scorer in [`compute/scorer/`](../../../compute/scorer/) runs on Fastly Compute and reads the matrix from a KV store at runtime. Operational background is in the [session scoring runbook](../../session_scoring_runbook.md).

Source: [`frontend/app/admin/session-scoring/page.tsx`](../../../frontend/app/admin/session-scoring/page.tsx) and [`frontend/components/SessionScoring/`](../../../frontend/components/SessionScoring/) (status, threshold slider, exclude-regex, matrix versions, labels, health, evaluation and top-flagged components).

## UI

- Status panel: enabled state, scorer service, matrix version, drift between the deployed and expected edge configuration, and Enable / Redeploy / Disable actions that stream provisioning progress. If prerequisites are missing, Enable shows a preflight instructions dialog instead of starting.
- Tabs: Overview, Labels, Matrix history, Audit.
- Overview: flag/enforce threshold slider, L2 enforcement card, exclude-regex card, scoring health, score distribution, compliance, ROC/PR curves, per-reason AUC, scorer latency/error and fail-open breakdown charts, and the top-flagged sessions table.
- Labels: ground-truth session labels created from the Sessions page flag action.
- Matrix history: stored matrix versions with restore; Retrain and Rotate key actions.
- Audit: operator actions on this service's scoring configuration.
- The page loads two composite bundles (`/scoring/config`, `/scoring/analytics`) and seeds the per-card query caches from them so child cards render without a request waterfall. Direct navigation falls back to `GET /api/services` for the active service.

Enforcement: L1/L2 scores are always computed and logged. L2 contributes to the enforced combined score only after an operator enables it (`l2-enforce`); enabling fades it in over three days, disabling returns it to observe-only. There is no clock-driven auto-ramp.

## Roles

Admin only. The routes use `require_admin`, and `_ANALYST_BLOCKED_SCORING_SUFFIXES` blocks `/config`, `/status`, `/audit`, `/threshold`, `/exclude-regex`, `/enforce-status-code`, `/enforce-threshold`, `/l2-enforce`, `/matrix-versions`, `/restore`, `/retrain`, `/rotate-key`, `/enable`, `/disable`, `/dashboard` and `/evaluation/per-reason` for analysts.

## Backend

Routers in [`backend/routers/session_scoring/`](../../../backend/routers/session_scoring/) (prefix `/api/services`, paths `/{service_id}/scoring/...`): `scoring_admin.py`, `scoring_analytics.py`, `scoring_vcl.py`. Models in [`backend/models/analytics/sessions.py`](../../../backend/models/analytics/sessions.py).

| Group | Endpoints |
|---|---|
| Composites | `GET config` (status, matrix versions, audit), `GET analytics` (`since_hours` 1 to 168), `GET dashboard` |
| Status and edge | `GET status`, `POST enable`, `POST disable` |
| Thresholds | `GET`/`PUT threshold`, `GET`/`PUT enforce-threshold`, `GET`/`PUT l2-enforce`, `GET`/`PUT enforce-status-code`, `GET`/`PUT exclude-regex`, `POST exclude-regex/validate` |
| Matrix and keys | `GET matrix-versions`, `POST matrix-versions/{version}/restore`, `POST retrain`, `POST rotate-key` |
| Analytics | `health`, `score-distribution`, `compliance-breakdown`, `latency-timeseries`, `evaluation`, `evaluation/per-reason`, `curves`, `threshold-preview`, `top-flagged`, `sessions/{sid}/events` (all `GET`) |
| Labels and audit | `GET`/`POST labels`, `PATCH`/`DELETE labels/{label_id}`, `GET audit` (`limit`, `since`) |

Deploy and teardown orchestration is in [`backend/provision/session_scoring/`](../../../backend/provision/session_scoring/) (KV store, config store, secret store, Wasm deploy, VCL snippets). The internal scoring sub-fetch carries `x-sigsci-skip-inspection-once` so NGWAF does not inspect the internal call; the VCL does not retry the sub-fetch and fails open on timeout or error. Matrix and history are stored under `meta/` in FOS beside the admin state. CLI equivalents: `python -m backend.provision.cli enable-scoring|disable-scoring --service-id <id>`.

## Tests

- E2E: `/admin/session-scoring — Admin Session Scoring Configuration Contract` in [`frontend/e2e/pages/admin-overview.spec.ts`](../../../frontend/e2e/pages/admin-overview.spec.ts)
- Backend: [`tests/routers/test_session_scoring_router.py`](../../../tests/routers/test_session_scoring_router.py), [`tests/scoring/test_scorer_and_matrix.py`](../../../tests/scoring/test_scorer_and_matrix.py), [`tests/provision/test_session_scoring_orchestrator.py`](../../../tests/provision/test_session_scoring_orchestrator.py)
