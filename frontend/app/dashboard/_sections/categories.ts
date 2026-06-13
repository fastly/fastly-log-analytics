import type { CardCategory, CardTint } from './types'

// Visible cards are rendered in this order, sectioned by category. Unknown card
// IDs (e.g. custom dashboard cards from bootstrap) fall through to "Custom" at
// the bottom. Categories with no visible cards are skipped entirely.
//
// `tint` pairs a subtle background + border + accent-dot color per section so
// each group reads as its own zone without overpowering the cards inside.
export const CARD_CATEGORIES: CardCategory[] = [
  {
    id: 'request',
    label: 'Request',
    cardIds: ['ip', 'asn', 'host', 'url', 'method', 'status', 'cache', 'proto', 'ua', 'referer'],
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
      '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind',
      'waf', 'waf_resp', 'waf_ms',
      'p_type', 'p_desc',
      'ja3', 'ja4', 'tls_ciphers_sha',
      'h2_fingerprint', 'oh_fingerprint',
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
]

export const CUSTOM_TINT: CardTint = {
  bg: 'bg-slate-50/60 dark:bg-slate-900/30',
  border: 'border-slate-200/60 dark:border-slate-800/50',
  dot: 'bg-slate-400',
}

export const CATEGORIZED_CARD_IDS = new Set(CARD_CATEGORIES.flatMap(c => c.cardIds))

export const COLLAPSED_SECTIONS_KEY = 'dashboard_collapsed_sections'
