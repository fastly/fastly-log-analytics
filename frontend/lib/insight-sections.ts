import { ShieldAlert, ServerCrash, Zap, Network, Globe, LayoutGrid, type LucideIcon } from 'lucide-react'

/**
 * Thematic sectioning for the Anomaly Detection (/insights) page.
 *
 * The backend owns the machine key (`category` on each insight card:
 * {security, origin, edge, network, traffic} — see backend/repositories/insights/
 * registry.py::InsightCategory). This file owns the *presentation*: the human
 * label, icon, and the fixed triage ordering. Keeping labels/order here means a
 * section can be renamed or reordered without touching the API contract or the
 * 30 insight definitions.
 *
 * Order is deliberate — triage flow: attack → broken → slow → shifting.
 */
export interface InsightSection {
  /** Stable machine key. Matches the backend `category` enum value. */
  key: string
  label: string
  description: string
  icon: LucideIcon
}

export const INSIGHT_SECTIONS: readonly InsightSection[] = [
  {
    key: 'security',
    label: 'Security & Threat Detection',
    description: 'Suspicious signatures, scanning, spoofing, proxy/abuse, and WAF activity.',
    icon: ShieldAlert,
  },
  {
    key: 'origin',
    label: 'Origin Health & Stability',
    description: 'Origin 5xx, latency/timeouts, retries, per-IP failures, and the shield path.',
    icon: ServerCrash,
  },
  {
    key: 'edge',
    label: 'Edge & Delivery Performance',
    description: 'Cache efficiency, edge/regional latency and tail, and delivery quality.',
    icon: Zap,
  },
  {
    key: 'network',
    label: 'Network Path',
    description: 'ISP / ASN reachability, RTT and packet loss, and metro/region delivery quality.',
    icon: Network,
  },
  {
    key: 'traffic',
    label: 'Traffic & Volumetrics',
    description: 'Volume swings, geography, composition, and automation.',
    icon: Globe,
  },
] as const

/**
 * Fallback bucket for any card whose `category` is missing or unknown to this
 * build. This is load-bearing for forward-compat: a backend that ships a new
 * category before the frontend knows it, a stale SSR seed, or a warm React
 * Query entry cached from before the category deploy would otherwise vanish.
 * Such cards land here instead of being dropped.
 */
export const OTHER_SECTION: InsightSection = {
  key: 'other',
  label: 'Other',
  description: 'Insights not yet assigned to a section.',
  icon: LayoutGrid,
}

const SECTION_BY_KEY = new Map<string, InsightSection>(INSIGHT_SECTIONS.map((s) => [s.key, s]))

/** Resolve a raw `category` string to its section, falling back to "Other". */
export function categoryToSection(category: string | null | undefined): InsightSection {
  if (category) {
    const section = SECTION_BY_KEY.get(category)
    if (section) return section
  }
  return OTHER_SECTION
}

// Severity ranking for within-section ordering. The compute response order is
// NOT guaranteed (parallel ThreadPoolExecutor + coalesced city/url aggregates
// append independently), so cards must be sorted explicitly or they shuffle
// between loads. Higher-urgency first; unknown severities sort last. The
// per-section rollup chips reuse this same order (worst → mildest), so the
// order is defined ONCE here and the rank is derived from it.
const SEVERITY_ORDER = ['critical', 'error', 'warning', 'info', 'clean'] as const
const SEVERITY_RANK: Record<string, number> = Object.fromEntries(
  SEVERITY_ORDER.map((sev, i) => [sev, i]),
)

/** Minimal shape the grouping/sorting helpers need — a subset of both the real
 * InsightCardData and the availability-derived skeleton entries, so the same
 * grouping drives both layouts (CLS: skeleton must mirror the loaded layout). */
export interface SectionableInsight {
  category?: string | null
  severity?: string | null
  title?: string | null
}

/** Stable sort by severity (critical→error→warning→info→clean), then title. */
export function sortInsights<T extends SectionableInsight>(cards: readonly T[]): T[] {
  return [...cards].sort((a, b) => {
    const ra = SEVERITY_RANK[a.severity ?? ''] ?? 99
    const rb = SEVERITY_RANK[b.severity ?? ''] ?? 99
    if (ra !== rb) return ra - rb
    return (a.title ?? '').localeCompare(b.title ?? '')
  })
}

/** One severity's count within a section, for the header rollup chips. */
export interface SeveritySummary {
  severity: string
  count: number
}

/**
 * Count a section's cards per severity, returned worst-first
 * (critical → error → warning → info → clean) with zero counts omitted.
 * Pure — drives the per-section rollup chips in the section header (e.g.
 * "2 critical · 3 warning"). Only the known severity ramp is summarised;
 * unknown severities and skeleton entries (no severity) are ignored, so a
 * loading section returns [] and shows no chips.
 */
export function summarizeSeverities(cards: readonly SectionableInsight[]): SeveritySummary[] {
  const counts = new Map<string, number>()
  for (const card of cards) {
    const sev = card.severity
    if (sev && sev in SEVERITY_RANK) {
      counts.set(sev, (counts.get(sev) ?? 0) + 1)
    }
  }
  return SEVERITY_ORDER.flatMap((severity) => {
    const count = counts.get(severity)
    return count ? [{ severity, count }] : []
  })
}

export interface InsightSectionGroup<T> {
  section: InsightSection
  cards: T[]
}

/**
 * Group cards by section and return them in the fixed INSIGHT_SECTIONS order
 * (with OTHER last), dropping empty sections. Each section's cards are sorted
 * via {@link sortInsights}. Works for both real insight cards and skeleton
 * availability entries.
 */
export function groupInsightsBySection<T extends SectionableInsight>(
  cards: readonly T[],
): InsightSectionGroup<T>[] {
  const buckets = new Map<string, T[]>()
  for (const card of cards) {
    const key = categoryToSection(card.category).key
    const bucket = buckets.get(key)
    if (bucket) bucket.push(card)
    else buckets.set(key, [card])
  }

  const groups: InsightSectionGroup<T>[] = []
  for (const section of [...INSIGHT_SECTIONS, OTHER_SECTION]) {
    const bucket = buckets.get(section.key)
    if (bucket && bucket.length > 0) {
      groups.push({ section, cards: sortInsights(bucket) })
    }
  }
  return groups
}
