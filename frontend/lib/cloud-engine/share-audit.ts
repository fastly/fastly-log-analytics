export type ShareAuditRow = {
  id: number;
  timestamp: string;
  event_type: string;
  email: string;
  ip_address: string;
  details: string;
};

// Cloud auth actions shown on Share › Audit, keyed to the High-Scale event names the panel
// filters on. `withTarget` marks actions whose target names something other than the actor.
const SHARE_EVENTS: Record<string, { event: string; withTarget: boolean }> = {
  invite_created: { event: "INVITE_CREATE", withTarget: true },
  invite_revoked: { event: "INVITE_REVOKE", withTarget: true },
  user_registered: { event: "INVITE_CLAIMED", withTarget: false },
  login: { event: "LOGIN_SUCCESS", withTarget: false },
  authentication_failure: { event: "LOGIN_FAIL", withTarget: true },
  role_changed: { event: "ROLE_CHANGED", withTarget: true },
  user_revoked: { event: "USER_REVOKED", withTarget: true },
  credential_revoked: { event: "CREDENTIAL_REVOKED", withTarget: true },
  setup_grant: { event: "OWNER_SETUP", withTarget: false },
};

const text = (value: unknown): string =>
  typeof value === "string" ? value : value == null ? "" : String(value);

// The admin_audit ops log also carries control-plane outbox rows (cron jobs, config updates);
// a row without domain or source predates those tags and can only be an auth row.
function isAuthRow(row: Record<string, unknown>): boolean {
  if (row.domain == null && row.source == null) return true;
  return row.domain === "auth" || row.source === "auth_archive";
}

export type CloudInviteEntry = {
  id: string;
  display_name?: string;
  role?: string;
  created_at?: number | string;
  expires_at?: number | string;
  accepted_at?: number | string | null;
  last_login_at?: number | string | null;
  revoked?: boolean;
};

export type CloudUserEntry = {
  id: string;
  display_name?: string;
  role?: string;
  created_at?: number | string;
  last_login_at?: number | string | null;
  revoked?: boolean;
  passkeys?: Array<{ id: string; created_at?: number | string }>;
};

export type CloudServiceEntry = {
  service_id: string;
  name?: string;
  service_name?: string;
};

function toIsoTimestamp(value: number | string | null | undefined): string | null {
  if (value == null) return null;
  if (typeof value === "number") {
    return new Date(value > 1e12 ? value : value * 1000).toISOString();
  }
  return String(value);
}

export function buildCloudShareStatusPayload(
  invitesJson: { invites?: CloudInviteEntry[] },
  usersJson: { users?: CloudUserEntry[] },
  servicesJson: { services?: CloudServiceEntry[] },
  originUrl: string | null,
) {
  const svcList = (servicesJson.services ?? []).map((s) => ({
    service_id: s.service_id,
    name: s.name || s.service_name || s.service_id,
  }));
  const allServiceIds = svcList.map((s) => s.service_id);
  const invites = (invitesJson.invites ?? [])
    .filter((i) => !i.revoked)
    .map((i) => {
      const role = i.role || "analyst";
      return {
        id: i.id,
        name: i.display_name || i.id,
        email: role,
        role,
        auth_method: "passkey",
        created_at: toIsoTimestamp(i.created_at) ?? new Date().toISOString(),
        expires_at: toIsoTimestamp(i.expires_at),
        last_login_at: toIsoTimestamp(i.last_login_at ?? i.accepted_at),
        service_ids: allServiceIds,
        pii_policy: { mask_ips: role === "analyst-no-pii" },
        allow_concurrent_sessions: true,
      };
    });
  const sessions = (usersJson.users ?? [])
    .filter((u) => !u.revoked)
    .map((u) => {
      const role = u.role || "analyst";
      return {
        session_id: u.id,
        invite_id: u.id,
        name: u.display_name || u.id,
        email: role,
        role,
        auth_method: `Passkey (${u.passkeys?.length ?? 0})`,
        ip_address: "—",
        last_active_time:
          toIsoTimestamp(u.last_login_at ?? u.created_at) ?? new Date().toISOString(),
      };
    });
  return {
    sharing_active: true,
    public_url: originUrl,
    active_session_count: sessions.length,
    max_concurrent_sessions: 100,
    invites,
    sessions,
    services: svcList,
    rate_limits: { lockouts: [], failures: [] },
  };
}

export function shareAuditRows(
  rows: ReadonlyArray<Record<string, unknown>>,
  opts: { from: number; to: number; eventType: string | null; email: string | null; limit: number },
): ShareAuditRow[] {
  const out: ShareAuditRow[] = [];
  const emailFilter = opts.email ? opts.email.toLowerCase() : null;
  for (const row of rows) {
    if (out.length >= opts.limit) break;
    const ts = Number(row.timestamp);
    if (!(ts >= opts.from && ts <= opts.to) || !isAuthRow(row)) continue;
    const mapped = SHARE_EVENTS[text(row.action)];
    if (!mapped) continue;
    const actor = text(row.actor);
    const displayName = text(row.display_name);
    const role = text(row.role);
    if (opts.eventType && mapped.event !== opts.eventType) continue;
    if (
      emailFilter &&
      !actor.toLowerCase().includes(emailFilter) &&
      !displayName.toLowerCase().includes(emailFilter) &&
      !role.toLowerCase().includes(emailFilter)
    ) {
      continue;
    }
    out.push({
      id: out.length + 1,
      timestamp: new Date(ts).toISOString(),
      event_type: mapped.event,
      email: actor,
      ip_address: text(row.ip) || "-",
      details: mapped.withTarget ? text(row.target) : "",
    });
  }
  return out;
}
