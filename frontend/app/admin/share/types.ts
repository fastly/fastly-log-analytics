"use client"

import * as React from "react"
import { parseAsStringLiteral } from "nuqs"

import { extractApiError } from "@/lib/api"
import type { Simplify, WithRequired, WithRequiredNullable } from "@/types/api"
import type { components } from "@/types/api.generated"

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

export type SortDir = 'asc' | 'desc'
export type SortValue = string | number | null | undefined
export type SortAccessors<T> = Record<string, (row: T) => SortValue>

export interface TableSort<T> {
  sorted: T[]
  sortKey: string
  sortDir: SortDir
  toggle: (key: string) => void
}

const isMissing = (v: SortValue): boolean => v === null || v === undefined || v === ''

function compareValues(a: SortValue, b: SortValue): number {
  if (typeof a === 'number' && typeof b === 'number') return a - b
  return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' })
}

export function useTableSort<T>(
  rows: T[],
  accessors: SortAccessors<T>,
  opts: { defaultKey: string; defaultDir?: SortDir },
): TableSort<T> {
  const [sortKey, setSortKey] = React.useState(opts.defaultKey)
  const [sortDir, setSortDir] = React.useState<SortDir>(opts.defaultDir ?? 'desc')

  const toggle = React.useCallback((key: string) => {
    setSortKey((prevKey) => {
      if (prevKey === key) {
        setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
        return prevKey
      }
      setSortDir('desc')
      return key
    })
  }, [])

  const sorted = React.useMemo(() => {
    const accessor = accessors[sortKey]
    if (!accessor) return rows
    const factor = sortDir === 'asc' ? 1 : -1
    return rows
      .map((row, i) => ({ row, i }))
      .sort((x, y) => {
        const av = accessor(x.row)
        const bv = accessor(y.row)
        const aMissing = isMissing(av)
        const bMissing = isMissing(bv)
        if (aMissing || bMissing) {
          if (aMissing && bMissing) {
            return x.i - y.i
          }
          return aMissing ? 1 : -1
        }
        const c = compareValues(av, bv)
        return c !== 0 ? c * factor : x.i - y.i
      })
      .map((d) => d.row)
  }, [rows, accessors, sortKey, sortDir])

  return { sorted, sortKey, sortDir, toggle }
}

export interface SortableHeadProps extends Omit<React.ComponentProps<'th'>, 'onClick'> {
  label: React.ReactNode
  sortKey: string
  activeKey: string
  dir: SortDir
  onSort: (key: string) => void
}

export const SHARE_TABS = ['invites', 'sessions', 'audit'] as const
export type ShareTab = (typeof SHARE_TABS)[number]
export const VALID_SHARE_TABS: ReadonlySet<string> = new Set(SHARE_TABS)

export const shareTabParser = parseAsStringLiteral(SHARE_TABS)
  .withDefault('invites')
  .withOptions({ clearOnDefault: true, history: 'replace', shallow: true })

export function normalizeShareTab(raw: string | null | undefined): ShareTab {
  return raw && VALID_SHARE_TABS.has(raw) ? (raw as ShareTab) : 'invites'
}

export const EVENT_TYPE_FILTER_OPTIONS = [
  'ALL', 'LOGIN_SUCCESS', 'LOGIN_FAIL', 'LOCKOUT', 'INVITE_CREATE', 'INVITE_REVOKE',
  'INVITE_DELETE', 'INVITE_CLAIMED', 'SESSION_BOOT', 'SESSION_TIMEOUT', 'TOS_ACCEPTED',
  'TUNNEL_START', 'TUNNEL_RESUMED', 'SHARE_START', 'SHARE_STOP', 'PANIC_TRIGGERED',
  'BACKUP_EXPORTED', 'BACKUP_IMPORTED', 'CLAIM_FAIL', 'OAUTH_AUTH_INIT',
  'OAUTH_CALLBACK_FAIL', 'OAUTH_VERIFY_FAIL', 'OAUTH_INVITE_NOT_FOUND', 'LOGIN_SUCCESS_OAUTH',
]

export type AuditFilters = { event_type: string; email: string; since: string; until: string }
