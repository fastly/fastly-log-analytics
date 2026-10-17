export type RateLimitFailure = { ip: string; count: number; window_s: number }
export type RateLimitLockout = { ip: string; remaining_s: number }
export type TunnelHistoryEntry = {
  started_at: string
  ended_at: string
  duration_s: number
  reason: string
}

import type { components } from '@/types/api.generated'
import type { Expect, Simplify, WireParity, WithRequired, WithRequiredNullable } from '@/types/api'

type InviteWire = components['schemas']['InviteRecord']
type ShareServiceWire = components['schemas']['ShareServiceEntry']
type ShareStatusWire = components['schemas']['ShareStatusResponse']

/**
 * A share invite as returned by /api/admin/share/status. Keys derive from the
 * generated InviteRecord; the overrides narrow what the producer always emits
 * (``last_login_at`` comes from the login audit events, see backend
 * ``get_last_login_by_email``).
 */
export type Invite = Simplify<
  Omit<WithRequired<InviteWire, 'id' | 'name' | 'email'>, 'auth_method' | 'pii_policy'> & {
    auth_method?: 'passcode' | 'oauth'
    pii_policy?: { mask_ips?: boolean } | null
  }
>

// The wire models these nested collections as free dicts (TunnelManager
// internals), so their shapes remain local narrowings.
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

export type ShareStatus = Simplify<
  Omit<
    WithRequiredNullable<
      ShareStatusWire,
      'public_endpoint' | 'public_url' | 'forward_port' | 'started_at'
    >,
    'sharing_active' | 'max_concurrent_sessions' | 'active_session_count' | 'services' | 'invites' | 'sessions' | 'audit_logs' | 'rate_limits' | 'telemetry'
  > &
    Pick<WithRequired<ShareStatusWire, 'sharing_active' | 'max_concurrent_sessions' | 'active_session_count'>, 'sharing_active' | 'max_concurrent_sessions' | 'active_session_count'> & {
      services: (WithRequired<ShareServiceWire, 'service_id' | 'name'>)[]
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
>

export type _InviteParity = Expect<WireParity<Invite, InviteWire>>
export type _ShareStatusParity = Expect<WireParity<ShareStatus, ShareStatusWire>>
