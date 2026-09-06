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
  services: {
    service_id: string
    name: string
    remote_frontend_deployed?: boolean
    sharing_domain?: string | null
    remote_service_id?: string | null
  }[]
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
import { formatDuration, formatDeterministicUTC } from '@/lib/date'

export function formatUptime(seconds: number | null | undefined): string {
  return formatDuration(seconds)
}

export function formatStamp(s: string | null | undefined): string {
  return formatDeterministicUTC(s)
}
