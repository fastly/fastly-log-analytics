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

export function shareAuditRows(
  rows: ReadonlyArray<Record<string, unknown>>,
  opts: { from: number; to: number; eventType: string | null; email: string | null; limit: number },
): ShareAuditRow[] {
  const out: ShareAuditRow[] = [];
  for (const row of rows) {
    if (out.length >= opts.limit) break;
    const ts = Number(row.timestamp);
    if (!(ts >= opts.from && ts <= opts.to) || !isAuthRow(row)) continue;
    const mapped = SHARE_EVENTS[text(row.action)];
    if (!mapped) continue;
    const actor = text(row.actor);
    if (opts.eventType && mapped.event !== opts.eventType) continue;
    if (opts.email && !actor.includes(opts.email)) continue;
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
