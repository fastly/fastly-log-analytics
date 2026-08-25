import { useMemo } from 'react'
import { useLogFieldsCatalog } from './useLogFieldsCatalog'
import { useBootstrap } from './useBootstrap'
import { CATEGORIZED_CARD_IDS } from '@/app/dashboard/_sections/categories'

export interface DashboardCard {
  id: string
  label: string
  inActiveFormat: boolean
}

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
    // them — they're client-derived from data the user already has (UA/IP for
    // bot lookup, waf_sig for signal split).
    const FORCE_VISIBLE = new Set(['_bot_name', '_ngwaf_bot_name', 'waf_sig_ind'])
    // Cards that ARE in an active group but are noisy and rarely wanted on
    // the default dashboard (per-request IDs, raw WAF tag blob).
    const FORCE_HIDDEN = new Set(['rid', 'prid', 'waf_sig', 'waf_req_id'])

    const activeIds = new Set<string>(((bootstrap as any).active_log_field_ids || []) as string[])

    const standardCards: DashboardCard[] = catalog.fields
      .filter(f => {
        if (excludedGroups.includes(f.group) || excludedIds.includes(f.id)) {
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

    const customCards: DashboardCard[] = ((bootstrap.custom_dashboard_cards || []) as { id: string, label: string }[])
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
      if (seen.has(c.id)) return false
      seen.add(c.id)
      return true
    })
  }, [catalog, bootstrap])
}
