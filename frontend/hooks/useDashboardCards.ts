import { useState, useCallback, useMemo } from 'react'
import { useLogFieldsCatalog } from './useLogFieldsCatalog'
import { useBootstrap } from './useBootstrap'
import { useMounted } from '@/hooks/useMounted'
import { parseFromInput } from '@/lib/date'
import type { PlotlyRelayoutEvent } from '@/types/plotly'
import { CATEGORIZED_CARD_IDS } from '@/app/dashboard/_sections/categories'

export interface DashboardCard {
  id: string
  label: string
  inActiveFormat: boolean
}

const NGWAF_CARD_IDS = new Set(['_ngwaf_bot_name', 'waf_sig_ind'])

// Numerical metrics, IPs, and high-cardinality IDs that do not support
// categorical top-N lists and are excluded from the aggregates endpoint on the backend.
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
  // Network quality (Group F/G)
  'tcp_rtt',
  'ploss',
  'rtt_min',
  'rtt_var',
  'retrans',
  'bw',
  'delivery_rate',
  'data_segs_out',
  // QUIC/HTTP3 (Group K)
  'q_rtt',
  'q_rtt_var',
  'q_lost',
  'q_cwnd',
  // Origin metrics (Group L)
  'ottfb',
  'ottlb',
  'oconnect_ms',
  'ost',
  'obytes',
  'oip',
  'oretries',
  // IO / other stats
  'io_input_bytes',
  'io_output_bytes',
  // Edge score / threat indicators
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

    // VIRTUAL fields aren't in any user-toggleable log group, so the
    // active_log_field_ids check below would hide them by default. Force-show
    // them when supported — NGWAF cards are filtered by attachment below.
    const FORCE_VISIBLE = new Set(['_bot_name', ...NGWAF_CARD_IDS])
    // Cards that ARE in an active group but are noisy and rarely wanted on
    // the default dashboard (per-request IDs, raw WAF tag blob).
    const FORCE_HIDDEN = new Set(['rid', 'prid', 'waf_sig', 'waf_req_id'])

    const activeIds = new Set<string>(bootstrap.active_log_field_ids ?? [])

    const standardCards: DashboardCard[] = catalog.fields
      .filter(f => {
        if ((f.group != null && excludedGroups.includes(f.group)) || excludedIds.includes(f.id)) {
          return false
        }

        const isCustom = f.is_custom || f.group === 'custom'
        if (isCustom) {
          // Check if it is a system-managed custom field
          const isSystemField =
            f.id.startsWith('cmcd_') ||
            f.id.startsWith('edge_') ||
            f.id.startsWith('rum_') ||
            f.id === 'fastly_req_id'

          if (isSystemField) {
            return false
          }

          // For actual user custom fields, only show if user asked to show on dashboard
          return !!f.show_in_dashboard
        } else {
          // For built-in fields, only show if they are explicitly categorized and support top-N
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
        // Exclude system fields from bootstrap custom cards too
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

interface CardVisibilityMigration {
  /** Bump when the set of IDs to retire or surface changes. Stored under `<storageKey>_mv`. */
  version: number
  /** IDs to strip from the saved set on first load after a version bump. */
  removeIds?: string[]
  /** IDs to insert into the saved set on first load after a version bump. */
  addIds?: string[]
}

/**
 * Persists a Set of visible card IDs to localStorage and keeps it in sync
 * across re-renders.
 *
 * @param storageKey   - The localStorage key to persist under.
 * @param allIds       - Every possible card ID (used to build the "show all" set).
 * @param defaultIds   - Optional subset of `allIds` to use as the initial visible set
 *                       (e.g., fields currently in the active log format). If omitted,
 *                       defaults to `allIds`.
 * @param migration    - Optional one-time prune. When `version` exceeds the stored
 *                       `<storageKey>_mv` value, `removeIds` are stripped from the
 *                       saved set and the new version is recorded. Lets us retire
 *                       cards from existing browsers without nuking other choices.
 */
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

  // SSR-safe: the hydration render ALWAYS uses the deterministic default set. Reading
  // localStorage there made the first client render diverge from the server's default-set
  // render (React #418 on /charts and the dashboard for users who had toggled cards). Once
  // mounted, the persisted value (with the migration applied) takes over; it re-derives
  // whenever `load` changes (e.g. allCards arriving from the catalog query). A user edit is
  // tied to the `load` it was made under, so a later `load` change re-reads storage (which
  // already holds the edit).
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
