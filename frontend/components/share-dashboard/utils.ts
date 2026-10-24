import * as React from 'react'

import { extractApiError } from '@/lib/api'
import type { components } from '@/types/api.generated'
import type { Simplify, WithRequired, WithRequiredNullable } from '@/types/api'

/**
 * Shared single-busy mutation runner for the share-dashboard panels.
 *
 * Wraps the repeated `setBusy(true) → await fn() → await onRefresh() →
 * catch onError(extractApiError(e)) → finally setBusy(false)` shape that
 * the boot/stop/panic/revoke handlers all share. Confirm() guards stay at
 * the call site (run() is invoked only after the user confirms).
 */
export function useShareMutation(
  onError: (msg: string) => void,
  onRefresh?: () => Promise<void> | void,
) {
  const [busy, setBusy] = React.useState(false)
  const run = async (fn: () => Promise<unknown>) => {
    setBusy(true)
    try {
      await fn()
      await onRefresh?.()
    } catch (e) {
      onError(extractApiError(e))
    } finally {
      setBusy(false)
    }
  }
  return { busy, run }
}

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
