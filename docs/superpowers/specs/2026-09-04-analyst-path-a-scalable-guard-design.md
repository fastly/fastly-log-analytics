# Analyst Path A Scalable-Topology Guard

## Status

Approved design; implementation pending.

## Context

Analyst Path A gives an independent dashboard instance read-only FOS
credentials and relies on the catalog being reconstructible from FOS. The
scalable Celery topology commits through a shared DuckLake catalog that is not
included in that credential payload, so issuing a Path A invite there creates a
configuration that cannot provide a complete independent dashboard.

Path A remains valid for synchronous/legacy deployments. Path B, the live
shared-instance analyst flow, is unaffected.

## Goals

- Fail closed for new and reused Path A flows on unsupported scalable
  deployments.
- Keep synchronous/legacy Path A behavior backward compatible.
- Use one backend-owned topology decision for API, CLI, and UI behavior.
- Surface an actionable explanation that directs operators to Path B.
- Avoid changing the Path B sharing and analyst-session trust model.

## Non-goals

- Exporting or synchronizing a FOS-resident DuckLake catalog.
- Removing Path A from synchronous/legacy deployments.
- Changing live shared-instance access.

## Architecture

The backend exposes a typed capability, `analyst_path_a_supported`, through
the existing service/bootstrap status payload. The value is derived from the
authoritative ingest topology (`INGEST_MODE`), not inferred from invite JSON or
frontend state. The capability includes a stable reason/message when false so
the UI can explain the restriction without duplicating topology rules.

A shared backend predicate is used by:

1. `POST /api/services/{service_id}/generate-viewer-key`.
2. The CLI analyst-invite command.
3. Any independent-join handler or stream before it starts provisioning work.

For `INGEST_MODE=celery`, invite and join operations return a typed 409 error.
The error explains that the shared DuckLake catalog is not present in the
FOS-only payload and recommends Path B. Synchronous/legacy operations continue
through the existing code path.

## Frontend behavior

The ProvisionWizard reads the capability from hydrated service/bootstrap state.
When Path A is unsupported, the Join option is disabled and an explanatory
message recommends live shared-instance access. Existing imported sync/legacy
JSON configs continue to work. The Invite Analyst dialog preserves its
existing backend-error rendering, which displays the actionable 409 message
when an invite request races with a topology change.

No Path B components, routes, cookies, middleware, or analyst-session logic are
changed.

## Error handling

Unsupported topology is a known conflict, not a server failure, so the API
uses HTTP 409 with the repository's standard error envelope. The CLI exits
non-zero with the same message. Join-time rejection happens before any FOS
access, local config write, or SSE success event, preventing silent empty
dashboards. Unexpected provisioning failures retain their existing error
handling.

## Testing

- Backend capability tests verify sync/legacy true and Celery false behavior.
- Router tests verify HTTP 409, the actionable Path B message, and unchanged
  404/403 behavior.
- CLI tests verify the shared guard and non-zero failure.
- Join tests verify rejection before side effects when the endpoint exists.
- Frontend tests verify enabled sync Join, disabled scalable Join, and the
  explanatory message.
- Path B regression tests remain unchanged and must continue to pass.
- OpenAPI artifacts are regenerated whenever the capability is added to an
  existing response model.

## Rollout and compatibility

This is a fail-closed compatibility guard, so no migration is required.
Existing sync/legacy invite payloads remain valid. Previously issued Celery
payloads cannot be made valid by this change; any future join implementation
must apply the same predicate rather than trusting a payload marker.
