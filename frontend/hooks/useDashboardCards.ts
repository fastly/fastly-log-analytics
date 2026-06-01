import { useMemo } from 'react'
import { useLogFieldsCatalog } from './useLogFieldsCatalog'
import { useBootstrap } from './useBootstrap'

export interface DashboardCard {
  id: string
  label: string
  inActiveFormat: boolean
}

export function useDashboardCards(): DashboardCard[] {
  const { data: bootstrap } = useBootstrap()
  const { data: catalog } = useLogFieldsCatalog()

  return useMemo(() => {
    if (!catalog?.fields || !bootstrap) return []

    const excludedGroups = ['METRICS', 'INTERNAL']
    const excludedIds = ['timestamp', 'elapsed', 'req_bytes', 'resp_bytes', 'req_header_bytes', 'ttfb', 'lat', 'lon']

    // VIRTUAL fields aren't in any user-toggleable log group, so the
    // active_log_field_ids check below would hide them by default. Force-show
    // them — they're client-derived from data the user already has (UA/IP for
    // bot lookup, waf_sig for signal split).
    const FORCE_VISIBLE = new Set(['_bot_name', '_ngwaf_bot_name', 'waf_sig_ind'])
    // Cards that ARE in an active group but are noisy and rarely wanted on
    // the default dashboard (per-request IDs, raw WAF tag blob).
    const FORCE_HIDDEN = new Set(['rid', 'prid', 'waf_sig', 'waf_req_id'])

    const activeIds = new Set<string>(((bootstrap as any).active_log_field_ids || []) as string[])

    const standardCards: DashboardCard[] = catalog.fields
      .filter(f => !excludedGroups.includes(f.group) && !excludedIds.includes(f.id))
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

    const customCards: DashboardCard[] = ((bootstrap.custom_dashboard_cards || []) as { id: string, label: string }[])
      .map(c => ({
        id: c.id,
        label: c.label,
        inActiveFormat: activeIds.size === 0 ? true : activeIds.has(c.id),
      }))

    const seen = new Set<string>()
    return [...standardCards, ...customCards].filter(c => {
      if (seen.has(c.id)) return false
      seen.add(c.id)
      return true
    })
  }, [catalog, bootstrap])
}
