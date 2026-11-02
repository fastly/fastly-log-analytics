'use client'

import { useState, useCallback, useMemo } from 'react'
import { useBootstrap, useLogFieldsCatalog, useMounted } from '@/hooks/useBootstrap'
import { parseFromInput } from '@/lib/utils'
import type { PlotlyRelayoutEvent } from '@/types/api'

export type CardTint = {
  bg: string
  border: string
  dot: string
}

export type CardCategory = {
  id: string
  label: string
  cardIds: string[]
  tint: CardTint
}

export const CARD_CATEGORIES: CardCategory[] = [
  {
    id: 'request',
    label: 'Request',
    cardIds: ['ip', 'asn', 'host', 'url', 'method', 'status', 'cache', 'proto', 'ua', 'referer', 'cookie_session', 'resp_header_content_encoding'],
    tint: { bg: 'bg-blue-50/60 dark:bg-blue-950/40', border: 'border-blue-200/70 dark:border-blue-900/60', dot: 'bg-blue-500' },
  },
  {
    id: 'cache',
    label: 'Cache',
    cardIds: ['ttl', 'age', 'hits', 'digest'],
    tint: { bg: 'bg-amber-50/60 dark:bg-amber-950/40', border: 'border-amber-200/70 dark:border-amber-900/60', dot: 'bg-amber-500' },
  },
  {
    id: 'geo',
    label: 'Geography',
    cardIds: ['city', 'region', 'country', 'metro'],
    tint: { bg: 'bg-emerald-50/60 dark:bg-emerald-950/40', border: 'border-emerald-200/70 dark:border-emerald-900/60', dot: 'bg-emerald-500' },
  },
  {
    id: 'network',
    label: 'Network & Connection',
    cardIds: [
      'tcp_rtt', 'transport', 'ploss', 'rtt_min', 'rtt_var', 'retrans',
      'c_speed', 'c_type', 'delivery_rate', 'data_segs_out',
    ],
    tint: { bg: 'bg-cyan-50/60 dark:bg-cyan-950/40', border: 'border-cyan-200/70 dark:border-cyan-900/60', dot: 'bg-cyan-500' },
  },
  {
    id: 'edge',
    label: 'Edge Infrastructure',
    cardIds: ['pop', 'backend', 'edge', 'server_region', 'tls', 'is_ipv6', 'conn_requests'],
    tint: { bg: 'bg-violet-50/60 dark:bg-violet-950/40', border: 'border-violet-200/70 dark:border-violet-900/60', dot: 'bg-violet-500' },
  },
  {
    id: 'security',
    label: 'Security',
    cardIds: [
      '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind', 'edge_score_reason_ind',
      'waf', 'waf_resp', 'waf_ms',
      'p_type', 'p_desc',
      'ja3', 'ja4', 'tls_ciphers_sha',
    ],
    tint: { bg: 'bg-rose-50/60 dark:bg-rose-950/40', border: 'border-rose-200/70 dark:border-rose-900/60', dot: 'bg-rose-500' },
  },
  {
    id: 'origin',
    label: 'Origin',
    cardIds: ['ottfb', 'ottlb', 'ost', 'obytes', 'oip', 'oretries'],
    tint: { bg: 'bg-yellow-50/60 dark:bg-yellow-950/40', border: 'border-yellow-200/70 dark:border-yellow-900/60', dot: 'bg-yellow-500' },
  },
  {
    id: 'quic',
    label: 'QUIC / HTTP3',
    cardIds: ['bw', 'q_rtt', 'q_rtt_var', 'q_lost', 'q_cwnd'],
    tint: { bg: 'bg-indigo-50/60 dark:bg-indigo-950/40', border: 'border-indigo-200/70 dark:border-indigo-900/60', dot: 'bg-indigo-500' },
  },
  {
    id: 'io',
    label: 'Image Optimization',
    cardIds: ['io_input_format', 'io_output_format'],
    tint: { bg: 'bg-fuchsia-50/60 dark:bg-fuchsia-950/40', border: 'border-fuchsia-200/70 dark:border-fuchsia-900/60', dot: 'bg-fuchsia-500' },
  },
]

export const CUSTOM_TINT: CardTint = {
  bg: 'bg-slate-50/60 dark:bg-slate-900/30',
  border: 'border-slate-200/60 dark:border-slate-800/50',
  dot: 'bg-slate-400',
}

export const CATEGORIZED_CARD_IDS = new Set(CARD_CATEGORIES.flatMap(c => c.cardIds))

export interface DashboardCard {
  id: string
  label: string
  inActiveFormat: boolean
}

const NGWAF_CARD_IDS = new Set(['_ngwaf_bot_name', 'waf_sig_ind'])

const NON_TOP_N_FIELDS = new Set([
  'timestamp',
  'elapsed',
  'ttfb',
  'req_bytes',
  'req_header_bytes',
  'resp_bytes',
  'lat',
  'lon',
  'rid',
  'prid',
  'waf_req_id',
  'waf_sig',
  '_source_file',
  'tcp_rtt',
  'ploss',
  'rtt_min',
  'rtt_var',
  'retrans',
  'bw',
  'delivery_rate',
  'data_segs_out',
  'q_rtt',
  'q_rtt_var',
  'q_lost',
  'q_cwnd',
  'ottfb',
  'ottlb',
  'oconnect_ms',
  'ost',
  'obytes',
  'oip',
  'oretries',
  'io_input_bytes',
  'io_output_bytes',
  'edge_score',
  'edge_score_l1',
  'edge_score_l2',
  'edge_score_rtt_us',
  'edge_score_exec_us',
])

export function useDashboardCards(): DashboardCard[] {
  const { data: bootstrap } = useBootstrap()
  const { data: catalog } = useLogFieldsCatalog()

  return useMemo(() => {
    if (!catalog?.fields || !bootstrap) return []

    const excludedGroups = ['METRICS', 'INTERNAL']
    const excludedIds = ['timestamp', 'elapsed', 'req_bytes', 'resp_bytes', 'req_header_bytes', 'ttfb', 'lat', 'lon']

    const FORCE_VISIBLE = new Set(['_bot_name', ...NGWAF_CARD_IDS])
    const FORCE_HIDDEN = new Set(['rid', 'prid', 'waf_sig', 'waf_req_id'])

    const activeIds = new Set<string>(bootstrap.active_log_field_ids ?? [])

    const standardCards: DashboardCard[] = catalog.fields
      .filter(f => {
        if ((f.group != null && excludedGroups.includes(f.group)) || excludedIds.includes(f.id)) {
          return false
        }

        const isCustom = f.is_custom || f.group === 'custom'
        if (isCustom) {
          const isSystemField =
            f.id.startsWith('cmcd_') ||
            f.id.startsWith('edge_') ||
            f.id.startsWith('rum_') ||
            f.id === 'fastly_req_id'

          if (isSystemField) {
            return false
          }

          return !!f.show_in_dashboard
        } else {
          return CATEGORIZED_CARD_IDS.has(f.id) && !NON_TOP_N_FIELDS.has(f.id)
        }
      })
      .map(f => {
        let inActiveFormat: boolean
        if (FORCE_HIDDEN.has(f.id)) inActiveFormat = false
        else if (FORCE_VISIBLE.has(f.id)) inActiveFormat = true
        else inActiveFormat = activeIds.size === 0 ? true : activeIds.has(f.id)
        return {
          id: f.id,
          label: f.label || f.id,
          inActiveFormat,
        }
      })

    const customCards: DashboardCard[] = (bootstrap.custom_dashboard_cards ?? [])
      .filter(c => {
        const isSystemField =
          c.id.startsWith('cmcd_') ||
          c.id.startsWith('edge_') ||
          c.id.startsWith('rum_') ||
          c.id === 'fastly_req_id'
        return !isSystemField
      })
      .map(c => ({
        id: c.id,
        label: c.label,
        inActiveFormat: activeIds.size === 0 ? true : activeIds.has(c.id),
      }))

    const seen = new Set<string>()
    return [...standardCards, ...customCards].filter(c => {
      if (NGWAF_CARD_IDS.has(c.id) && bootstrap.ngwaf_configured !== true) return false
      if (seen.has(c.id)) return false
      seen.add(c.id)
      return true
    })
  }, [catalog, bootstrap])
}

export function usePlotlyRangeZoom(
  timezone: string,
  setRange: (start: string, end: string) => void,
) {
  return useCallback(
    (event: PlotlyRelayoutEvent) => {
      if (event['xaxis.autorange'] === true || event['xaxis.showspikes'] !== undefined) return

      const nested = Array.isArray(event['xaxis.range']) ? event['xaxis.range'] : []
      const bound = (flat: unknown, fallback: unknown): string | number | undefined => {
        const v = flat ?? fallback
        return typeof v === 'string' || typeof v === 'number' ? v : undefined
      }
      const x0 = bound(event['xaxis.range[0]'], nested[0])
      const x1 = bound(event['xaxis.range[1]'], nested[1])

      if (x0 === undefined || x1 === undefined) return

      try {
        const toLocalStr = (val: string | number) => {
          if (typeof val === 'number') {
            const d = new Date(val)
            const pad = (n: number) => n.toString().padStart(2, '0')
            return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
          }
          return val.replace(' ', 'T')
        }
        const parsedStart = parseFromInput(toLocalStr(x0), timezone)
        const parsedEnd = parseFromInput(toLocalStr(x1), timezone)
        if (parsedStart && parsedEnd) {
          setRange(parsedStart, parsedEnd)
        }
      } catch {
        // Ignore unparseable zoom events
      }
    },
    [setRange, timezone],
  )
}

export interface CardVisibilityMigration {
  version: number
  removeIds?: string[]
  addIds?: string[]
}

export function useCardVisibility(
  storageKey: string,
  allIds: string[],
  defaultIds?: string[],
  migration?: CardVisibilityMigration,
) {
  const allIdsStr = JSON.stringify(allIds)
  const defaultIdsStr = JSON.stringify(defaultIds ?? allIds)
  const allVisible = useMemo(() => new Set<string>(JSON.parse(allIdsStr)), [allIdsStr])
  const defaultVisible = useMemo(() => new Set<string>(JSON.parse(defaultIdsStr)), [defaultIdsStr])

  const migrationKey = `${storageKey}_mv`
  const migrationVersion = migration?.version
  const migrationRemoveStr = JSON.stringify(migration?.removeIds ?? [])
  const migrationAddStr = JSON.stringify(migration?.addIds ?? [])

  const load = useCallback((): Set<string> => {
    if (typeof window === 'undefined') return defaultVisible
    try {
      const stored = localStorage.getItem(storageKey)
      if (!stored) return defaultVisible

      const set = new Set<string>(JSON.parse(stored))

      if (migrationVersion !== undefined) {
        const storedVersion = Number(localStorage.getItem(migrationKey) ?? 0)
        if (storedVersion < migrationVersion) {
          const removeIds: string[] = JSON.parse(migrationRemoveStr)
          const addIds: string[] = JSON.parse(migrationAddStr)
          for (const id of removeIds) set.delete(id)
          for (const id of addIds) set.add(id)
          try {
            localStorage.setItem(storageKey, JSON.stringify([...set]))
            localStorage.setItem(migrationKey, String(migrationVersion))
          } catch {}
        }
      }

      return set
    } catch {}
    return defaultVisible
  }, [storageKey, defaultVisible, migrationKey, migrationVersion, migrationRemoveStr, migrationAddStr])

  const mounted = useMounted()
  const [edit, setEdit] = useState<{ source: typeof load; set: Set<string> } | null>(null)
  const stored = useMemo(() => (mounted ? load() : defaultVisible), [mounted, load, defaultVisible])
  const visibleCards = edit && edit.source === load ? edit.set : stored

  const toggleCard = useCallback((id: string) => {
    setEdit(prev => {
      const next = new Set(prev && prev.source === load ? prev.set : stored)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      try { localStorage.setItem(storageKey, JSON.stringify([...next])) } catch {}
      return { source: load, set: next }
    })
  }, [storageKey, load, stored])

  const showAll = useCallback(() => {
    setEdit({ source: load, set: allVisible })
    try { localStorage.setItem(storageKey, JSON.stringify([...allVisible])) } catch {}
  }, [storageKey, allVisible, load])

  const reset = useCallback(() => {
    setEdit({ source: load, set: defaultVisible })
    try { localStorage.setItem(storageKey, JSON.stringify([...defaultVisible])) } catch {}
  }, [storageKey, defaultVisible, load])

  return { visibleCards, toggleCard, showAll, reset, defaultVisible }
}
