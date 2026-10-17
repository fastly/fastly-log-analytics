import type { components } from '@/types/api.generated'
import type { Simplify, WithRequired, WithRequiredNullable } from '@/types/api'

type InviteWire = components['schemas']['InviteRecord']
type ShareServiceWire = components['schemas']['ShareServiceEntry']
type ShareStatusWire = components['schemas']['ShareStatusResponse']

/**
 * A share invite as returned by /api/admin/share/status. Keys derive from the
 * generated InviteRecord; the overrides narrow what the producer always emits
 * (``last_login_at`` comes from the login audit events, see backend
 * ``get_last_login_by_email``).
 */
export type Invite = WithRequired<InviteWire, 'id' | 'name' | 'email'>

export type ShareSession = components['schemas']['ShareSessionRecord']
export type AuditLog = components['schemas']['ShareAuditLogRow']
export type RateLimitFailure = components['schemas']['ShareRateLimitFailure']
export type RateLimitLockout = components['schemas']['ShareRateLimitLockout']
export type TunnelHistoryEntry = components['schemas']['ShareTunnelUptimeEntry']

export type ShareStatus = Simplify<
  Omit<
    WithRequiredNullable<ShareStatusWire, 'public_endpoint' | 'public_url' | 'forward_port' | 'started_at'>,
    'sharing_active' | 'max_concurrent_sessions' | 'active_session_count' | 'services' | 'invites' | 'sessions' | 'audit_logs'
  > &
    Pick<
      WithRequired<ShareStatusWire, 'sharing_active' | 'max_concurrent_sessions' | 'active_session_count'>,
      'sharing_active' | 'max_concurrent_sessions' | 'active_session_count'
    > & {
      services: WithRequired<ShareServiceWire, 'service_id' | 'name'>[]
      invites: Invite[]
      sessions: ShareSession[]
      audit_logs: AuditLog[]
    }
>
