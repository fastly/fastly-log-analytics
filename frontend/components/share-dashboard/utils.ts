export type RateLimitFailure = { ip: string; count: number; window_s: number }
export type RateLimitLockout = { ip: string; remaining_s: number }
export type TunnelHistoryEntry = {
  started_at: string
  ended_at: string
  duration_s: number
  reason: string
}

/**
 * A share invite as returned by /api/admin/share/status. The endpoint returns
 * a plain dict (no response_model), so this is a hand-authored mirror — keep it
 * in sync with ``build_share_status`` / ``get_remote_invites`` in the backend.
 * ``last_login_at`` is derived server-side from the login audit events (see
 * backend ``get_last_login_by_email``).
 */
export type Invite = {
  id: string
  name: string
  email: string
  service_ids?: string[]
  expires_at?: string | null
  created_at?: string
  revoked?: number | boolean
  allow_concurrent_sessions?: boolean
  auth_method?: 'passcode' | 'oauth'
  oauth_provider?: string | null
  pii_policy?: { mask_ips?: boolean } | null
  last_login_at?: string | null
}

export type ShareSession = {
  session_id: string
  invite_id: string
  name?: string
  email: string
  ip_address: string
  auth_method?: 'passcode' | 'oauth'
  oauth_provider?: string | null
  login_time?: string
  last_active_time?: string
}

export type AuditLog = {
  id?: number
  timestamp: string
  event_type: string
  email?: string | null
  ip_address?: string
  details?: string
}

export type ShareStatus = {
  sharing_active: boolean
  public_endpoint: string | null
  public_url: string | null
  forward_port: number | null
  started_at: string | null
  max_concurrent_sessions: number
  active_session_count: number
  services: { service_id: string; name: string }[]
  invites: Invite[]
  sessions: ShareSession[]
  audit_logs: AuditLog[]
  rate_limits?: { failures: RateLimitFailure[]; lockouts: RateLimitLockout[] }
  telemetry?: {
    heartbeat_unauth_count: number
    current_uptime_s: number | null
    tunnel_uptime_history: TunnelHistoryEntry[]
  }
}

export function formatUptime(seconds: number | null | undefined): string {
  if (seconds == null || seconds < 0) return '—'
  if (seconds < 60) return `${seconds}s`
  const m = Math.floor(seconds / 60)
  if (m < 60) return `${m}m ${seconds % 60}s`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ${m % 60}m`
  const d = Math.floor(h / 24)
  return `${d}d ${h % 24}h`
}

export function formatStamp(s: string | null | undefined): string {
  if (!s) return '—'
  try {
    // Deterministic across server and client: pin BOTH the locale and the
    // timezone, AND format the date and time parts SEPARATELY, joining with a
    // literal separator we control. A single toLocaleString() with both date
    // and time fields emits a locale-pattern CONNECTOR between them, and that
    // connector differs across ICU/CLDR versions — Node (SSR) renders
    // "Jun 11, 2026, 10:04 PM" while WebKit renders "Jun 11, 2026 at 10:04 PM".
    // Since /admin/share's share_status is dehydrated into the SSR HTML, that
    // drift threw React #418 on the invites/sessions/audit tables (webkit-
    // only). Splitting the calls sidesteps the connector entirely. UTC is the
    // right frame for an admin coordinating share access across timezones.
    const d = new Date(s)
    const datePart = d.toLocaleDateString('en-US', {
      timeZone: 'UTC',
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    })
    const timePart = d.toLocaleTimeString('en-US', {
      timeZone: 'UTC',
      hour: '2-digit',
      minute: '2-digit',
    })
    return `${datePart}, ${timePart} UTC`
  } catch {
    return s
  }
}
