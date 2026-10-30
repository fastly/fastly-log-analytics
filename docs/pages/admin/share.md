# Live Share Admin (`/admin/share`)

Admin page for sharing the running dashboard with remote analysts ("Remote Dashboard Sharing"): start and stop sharing, manage invites, watch and boot sessions, review the audit trail. The analyst side is [share-login.md](../share-login.md).

Source: [`frontend/app/admin/share/`](../../../frontend/app/admin/share/) (`page.tsx`, `SharingControlPanel.tsx`, `InvitationsPanel.tsx`, `ShareSessionsAndAuditPanels.tsx`, `CreateInviteDialog.tsx`). The remote-frontend deploy dialog lives in [`frontend/components/DeployRemoteFrontendDialog/`](../../../frontend/components/DeployRemoteFrontendDialog/).

## URL parameters

`tab`: `invites` (default), `sessions`, `audit`. Invalid values fall back to `invites`; the parameter is written back as the tab changes.

## UI

- Sharing control panel: ACTIVE/OFF status, active versus maximum concurrent sessions, public HTTPS endpoint, Start, Stop and Panic (stops sharing and boots every session), plus deploy/teardown of a remote frontend service.
- Invitations: create (name, email, service selection, expiry duration, optional IP whitelist, PII policy, optional query-window limits, passcode or SSO provider), copy the one-shot claim link, change passcode, edit service scope, PII policy and sharing options, revoke, reactivate, delete.
- Sessions: live analyst sessions with service scope, IP attribution and per-session activity, and a Boot action.
- Audit: filterable `remote_share_audit_logs`.
- Live updates arrive on the `share` channel of the multiplexed admin stream (`GET /api/admin/events/stream`).

Sharing works only in direct mode against an HTTPS `public_endpoint` (an admin-provided hostname or IP); the old SSH-tunnel option no longer exists. Analysts never receive storage credentials; middleware blocks admin paths for remote requests and applies response hardening (CSP, `X-Frame-Options: DENY`, no-store, no-referrer).

## Roles

Admin only. Every route below is under `require_admin`, `/api/admin/` is analyst-blocked, and the page is not reachable over the share tunnel.

## Backend

Routers in [`backend/routers/share_admin/`](../../../backend/routers/share_admin/) (`tunnel_routes.py`, `invites.py`, `sessions_and_tos.py`), mounted at `/api/admin/share`; models in [`backend/models/admin/sharing.py`](../../../backend/models/admin/sharing.py).

| Group | Endpoints (under `/api/admin/share`) |
|---|---|
| State | `GET /status`, `GET /live`, `GET /banner`, `GET /audit-logs` |
| Lifecycle | `POST /start` (`public_endpoint`, `forward_port`), `POST /stop`, `POST /panic` |
| Invites | `GET`/`POST /invites`; `PATCH /invites/{invite_id}/services`, `/pii`, `/sharing`, `/passcode`; `POST /invites/{invite_id}/regenerate-passcode`, `/revoke`, `/reactivate`, `/claim-token`; `DELETE /invites/{invite_id}` |
| Sessions | `POST /sessions/{session_id}/boot`, `/revoke` |
| Settings and terms | `GET`/`PATCH /settings` (maximum concurrent sessions), `GET`/`POST`/`PUT /tos`, `GET /wordphrase`, `GET /oauth-providers` |
| Data handling | `POST /backup/export`, `POST /backup/import`, `POST /gdpr/erase`, `POST /purge` |
| Remote frontend | `POST /api/sharing/deploy-frontend`, `POST /api/sharing/teardown-frontend` (admin only, same router file) |

Share state (`remote_invites`, `invite_services`, `remote_sessions`, `remote_share_audit_logs`, `share_settings`, `share_tos_versions`, claim tokens) is in PostgreSQL via [`backend/core/share_db/`](../../../backend/core/share_db/); the runtime (sessions, per-IP login rate limiting, sever-all) is `TunnelManager` in [`backend/utils/tunnel/`](../../../backend/utils/tunnel/). Analyst sessions use an idle timeout that is refreshed only by genuine interaction (`X-User-Active`) and the heartbeat. See [AGENTS.md](../../../AGENTS.md) (Live Dashboard Sharing) for the activity model.

## Tests

- E2E: `/admin/share — Admin Live Share Management Contract` in [`frontend/e2e/pages/admin-overview.spec.ts`](../../../frontend/e2e/pages/admin-overview.spec.ts)
- Backend: [`tests/remote_access/test_middleware.py`](../../../tests/remote_access/test_middleware.py), [`tests/remote_access/test_share_db.py`](../../../tests/remote_access/test_share_db.py), [`tests/remote_access/test_oauth.py`](../../../tests/remote_access/test_oauth.py)
