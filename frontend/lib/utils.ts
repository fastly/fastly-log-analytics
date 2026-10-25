import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"
import { ShieldAlert, ServerCrash, Zap, Network, Globe, LayoutGrid, type LucideIcon } from 'lucide-react'
import { formatDate, toUTCDate } from '@/lib/format'
import { CHART_COLORS, densifyBarSeries, type BarSeriesPoint } from '@/lib/chart-helpers'
import { useFilterStore } from '@/stores/filterStore'
import type { components } from '@/types/api.generated'
import type { FiltersPayload, PlotlyTrace } from '@/types/api'


export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatDateTime(isoString: string | null | undefined, timeZone?: string): string {
  if (!isoString) return 'Unknown'
  return formatDate(isoString, timeZone || 'UTC', 'MMM d, yyyy h:mm:ss a')
}

export function downloadBlob(blob: Blob, filename: string) {
  const url = window.URL.createObjectURL(blob)
  const a = Object.assign(document.createElement('a'), {
    href: url,
    download: filename,
  })
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  window.URL.revokeObjectURL(url)
}

export function downloadAsCsv(rows: Record<string, unknown>[], columns: string[], filename: string) {
  const escape = (v: unknown): string => {
    if (v == null) return ''
    const s = String(v)
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s
  }
  const csv = [
    columns.map(escape).join(','),
    ...rows.map(r => columns.map(c => escape(r[c])).join(',')),
  ].join('\n')
  downloadBlob(new Blob([csv], { type: 'text/csv;charset=utf-8;' }), filename)
}

/**
 * Copy text to the system clipboard, returning true on success and false on
 * failure (e.g. non-secure context or permission denied).
 */
export async function copyToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text)
    return true
  } catch {
    return false
  }
}

/**
 * Pair each item with a React key built from its own label plus the number of earlier items
 * that share it. For positional data with no natural id (table cells, repeated labels) this
 * keeps keys stable and unique without keying on the array index.
 */
export function keyByOccurrence<T>(items: readonly T[], label: (item: T) => string): { key: string; item: T }[] {
  const seen = new Map<string, number>()
  return items.map((item) => {
    const base = label(item)
    const n = seen.get(base) ?? 0
    seen.set(base, n + 1)
    return { key: n === 0 ? base : `${base}#${n}`, item }
  })
}

// Runtime guards for admin payloads whose generated schema is a free dict.
// Callers narrow to a local shape only after the top-level container checks
// out, so a malformed body degrades to the caller's fallback instead of
// throwing deep inside render.

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

export function recordOrNull<T extends object>(v: T | null | undefined): T | null {
  return isRecord(v) ? (v as T) : null
}

export function arrayOrEmpty<T>(v: readonly T[] | null | undefined): T[] {
  return Array.isArray(v) ? [...v] : []
}

/** Narrow a plain string (e.g. a Select's value) to one member of a generated string-enum union. */
export function oneOf<T extends string>(allowed: readonly T[], value: string | null | undefined): T | undefined {
  return allowed.find((a) => a === value)
}

/**
 * Client-IP-family filter columns.
 *
 * When PII masking is enabled for an analyst (invite `pii_policy.mask_ips`),
 * these columns are non-filterable: display masking is response-side only, so
 * letting an analyst filter by an exact IP would be a presence oracle (and a
 * dead-end on the masked value anyway). The frontend hides the drill-down
 * affordances for these; the real guarantee is the server-side lock in
 * `backend/utils/remote_access.py` (`_PII_FORBIDDEN_FILTER_COLS`), which 403s
 * any such filter. Keep this list in sync with that set.
 *
 * `oip` (origin / CDN shield IP) is intentionally NOT here — it's operator
 * infra, not end-user PII, and the Origin IP-Health card depends on filtering
 * by it.
 */
export const IP_FAMILY_FIELDS = ['ip', 'client_ip', 'ip_address', 'remote_addr'] as const

/** True when `column` is a client-IP-family field that masking analysts may not filter on. */
export function isIpFamilyField(column: string): boolean {
  return (IP_FAMILY_FIELDS as readonly string[]).includes(column)
}

// Cookie names and helpers shared between SSR Server Components and client stores.
// No 'use client' directive — imported by both Server Components (app/layout.tsx)
// and Client Components (AppLayout.tsx, StoreHydrator.tsx, DebugPanel.tsx).
// Adding 'use client' here would wrap string constants in Next's client-reference
// proxy so they evaluate to `undefined` inside Server Components.

export const ACTIVE_SERVICE_COOKIE = 'fla.activeServiceId'

export function setActiveServiceCookie(serviceId: string | null) {
  if (typeof document === 'undefined') return
  if (serviceId && /^[A-Za-z0-9]{10,30}$/.test(serviceId)) {
    document.cookie = `${ACTIVE_SERVICE_COOKIE}=${serviceId}; path=/; max-age=31536000; samesite=lax`
  } else {
    document.cookie = `${ACTIVE_SERVICE_COOKIE}=; path=/; max-age=0; samesite=lax`
  }
}

export const DEBUG_RESPONSES_COOKIE = 'fla.debugResponses'

export const SIDEBAR_COLLAPSED_COOKIE = 'fla.sidebarCollapsed'

export function buildServiceHref(path: string, serviceId: string | null | undefined): string {
  if (!serviceId) return path
  const sep = path.includes('?') ? '&' : '?'
  return `${path}${sep}service=${encodeURIComponent(serviceId)}`
}

export function buildStreamDetailUrl(token: string, serviceId?: string | null): string {
  const base = `/sessions/stream?token=${encodeURIComponent(token)}`
  return serviceId ? `${base}&service=${encodeURIComponent(serviceId)}` : base
}

export function buildSessionDashboardUrl(
  serviceId: string | null | undefined,
  filterKey: string,
  filterValue: string | null | undefined,
  sessionStart: string | null | undefined,
  sessionEnd: string | null | undefined,
): string {
  const params = new URLSearchParams()
  if (serviceId != null) params.set('service', serviceId)
  if (filterValue != null) params.set(`filter_${filterKey}`, filterValue)
  if (sessionStart != null) params.set('start_time', sessionStart)
  if (sessionEnd != null) params.set('end_time', sessionEnd)
  return `/dashboard?${params.toString()}`
}

/**
 * Shared className strings for the "panel" dialog pattern used across
 * the app's larger modals (CronSettingsModal, InviteAnalystDialog,
 * PopLocationsModal, TeardownDialog, SyncFromCloudModal,
 * CreateInviteDialog, LogSettingsModal, ProvisionWizard, SSEModal).
 *
 * Pattern: fixed-max-height panel with flex column layout, scrollable
 * body sandwiched between a sticky header (border-b) and footer
 * (border-t bg-muted/10). The body itself is left to each caller since
 * padding / scroll behavior varies (some use ScrollArea, some plain
 * overflow-y-auto, some inline form layouts).
 *
 * Headers come in two flavors:
 *  - `panelDialogHeaderSolid` (bg-background): used when the header has
 *    busy UI (badges, step indicators) and needs more visual weight.
 *  - `panelDialogHeaderMuted` (bg-muted/10): the default for simpler
 *    headers — pairs visually with the matching footer background.
 *
 * Compose with `cn()` so callers can append width / showCloseButton /
 * conditional classes without fighting tailwind-merge ordering.
 */

export const panelDialogContent =
  "max-h-[90vh] flex flex-col p-0 overflow-hidden"

export const panelDialogHeaderMuted =
  "px-6 pt-6 pb-4 border-b bg-muted/10 shrink-0"

export const panelDialogHeaderSolid =
  "px-6 pt-6 pb-4 border-b bg-background shrink-0"

export const panelDialogFooter =
  "px-6 py-4 border-t bg-muted/10 shrink-0"

// ---------------------------------------------------------------------------
// User activity tracking (folded from userActivity.ts)
// ---------------------------------------------------------------------------

export const ACTIVE_WINDOW_MS = 120_000
const MOUSE_THROTTLE_MS = 1_000

let lastInteractionAt = typeof window !== 'undefined' ? Date.now() : 0
let lastThrottledBump = 0
let listenersAttached = false

function bump() {
  lastInteractionAt = Date.now()
}

function bumpThrottled() {
  const now = Date.now()
  if (now - lastThrottledBump < MOUSE_THROTTLE_MS) return
  lastThrottledBump = now
  lastInteractionAt = now
}

function ensureListeners() {
  if (listenersAttached || typeof window === 'undefined' || typeof window.addEventListener !== 'function') return
  listenersAttached = true
  window.addEventListener('mousemove', bumpThrottled, { passive: true })
  window.addEventListener('wheel', bumpThrottled, { passive: true })
  window.addEventListener('keydown', bump)
  window.addEventListener('pointerdown', bump, { passive: true })
  window.addEventListener('touchstart', bump, { passive: true })
}

ensureListeners()

export function msSinceLastInteraction(): number {
  if (typeof window === 'undefined') return 0
  ensureListeners()
  return Date.now() - lastInteractionAt
}

export function isUserActive(windowMs: number = ACTIVE_WINDOW_MS): boolean {
  if (typeof window === 'undefined') return true
  ensureListeners()
  return Date.now() - lastInteractionAt < windowMs
}

export function __resetForTests(at: number = 0): void {
  lastInteractionAt = at
  lastThrottledBump = 0
}

// ---------------------------------------------------------------------------
// Shared constants (folded from constants.ts)
// ---------------------------------------------------------------------------

export const INTERVALS = [
  { label: '1s', value: '1 second' },
  { label: '1m', value: '1 minute' },
  { label: '1h', value: '1 hour' },
  { label: '1d', value: '1 day' },
] as const

export type ChartInterval = typeof INTERVALS[number]['value']

export const INTERVAL_SECONDS: Record<ChartInterval, number> = {
  '1 second': 1, '1 minute': 60, '1 hour': 3600, '1 day': 86400,
}

export const TRENDS = [
  { label: 'Off', value: 'off' }, { label: 'Auto', value: 'auto' },
  { label: '1m', value: '1m' }, { label: '5m', value: '5m' },
  { label: '1h', value: '1h' }, { label: '1d', value: '1d' },
]

export const CHART_LAYOUT_DEFAULTS = {
  tickformatstops: [
    { dtickrange: [0, 1000], value: "%H:%M:%S.%f" },
    { dtickrange: [1000, 60000], value: "%H:%M:%S" },
    { dtickrange: [60000, 3600000], value: "%H:%M" },
    { dtickrange: [3600000, 86400000], value: "%H:%M<br>%b %d" },
    { dtickrange: [86400000, 604800000], value: "%b %d" },
    { dtickrange: [604800000, "M1"], value: "%b %d" },
    { dtickrange: ["M1", "M12"], value: "%b %Y" },
    { dtickrange: ["M12", null], value: "%Y" }
  ]
}

export const SHIELD_MAP: Record<string, string> = {
  "us-east-1": "iad-va-us", "us-west": "bfi-wa-us", "us-central-1": "chi-il-us",
  "eu-central-1": "frankfurt-de", "eu-south-1": "mxp-milan-it", "eu-west-1": "mxp-paris-fr",
  "uk-east-1": "london-uk", "jp-central-1": "nrt-tokyo-jp", "au-east-1": "sydney-au",
};

export const REGION_LABELS: Record<string, string> = {
  "us-east-1": "US East (Ashburn)", "us-west": "US West (Seattle)", "us-central-1": "US Central (Chicago)",
  "eu-central-1": "EU Central (Frankfurt)", "eu-south-1": "EU South (Milan)", "eu-west-1": "EU West (Paris)",
  "uk-east-1": "UK East (London)", "jp-central-1": "JP Central (Tokyo)", "au-east-1": "AU East (Sydney)",
};

export const SHIELD_LABELS: Record<string, string> = {
  none: "None", "iad-va-us": "IAD (Ashburn)", "bfi-wa-us": "BFI (Seattle)", "chi-il-us": "CHI (Chicago)",
  "frankfurt-de": "FRA (Frankfurt)", "mxp-milan-it": "MXP (Milan)", "mxp-paris-fr": "MXP (Paris)",
  "london-uk": "LHR (London)", "nrt-tokyo-jp": "NRT (Tokyo)", "sydney-au": "SYD (Sydney)",
};

export const PERIOD_LABELS: Record<string, string> = {
  "1": "1 second", "5": "5 seconds", "10": "10 seconds", "20": "20 seconds", "30": "30 seconds",
  "60": "1 minute", "120": "2 minutes", "300": "5 minutes", "600": "10 minutes", "900": "15 minutes",
};

export const RETENTION_OPTIONS = [
  { value: "1", label: "1 day" }, { value: "3", label: "3 days" }, { value: "7", label: "7 days" },
  { value: "14", label: "14 days" }, { value: "30", label: "30 days" }, { value: "90", label: "90 days" },
  { value: "180", label: "180 days" }, { value: "365", label: "365 days" }, { value: "0", label: "Forever" },
] as const;

// ---------------------------------------------------------------------------
// Dependency-free DOM toast helper (folded from toast.ts)
// ---------------------------------------------------------------------------

type ToastKind = 'info' | 'success' | 'error' | 'warn'
interface ToastOptions { durationMs?: number }
interface ToastActionOptions extends ToastOptions {
  actionLabel: string
  onAction: () => void
  kind?: ToastKind
}

const PALETTE: Record<ToastKind, { bg: string; border: string; fg: string }> = {
  info: { bg: '#0f172a', border: '#1e293b', fg: '#e2e8f0' },
  success: { bg: '#064e3b', border: '#065f46', fg: '#ecfdf5' },
  error: { bg: '#7f1d1d', border: '#991b1b', fg: '#fef2f2' },
  warn: { bg: '#78350f', border: '#92400e', fg: '#fffbeb' },
}

let stackContainer: HTMLDivElement | null = null
const recentMessages = new Map<string, number>()
const RECENT_DEDUP_MS = 1500

function ensureContainer(): HTMLDivElement | null {
  if (typeof document === 'undefined') return null
  if (stackContainer && document.body.contains(stackContainer)) return stackContainer
  const el = document.createElement('div')
  el.setAttribute('role', 'region')
  el.setAttribute('aria-label', 'Notifications')
  el.style.cssText = 'position:fixed;top:16px;right:16px;z-index:2147483647;display:flex;flex-direction:column;gap:8px;pointer-events:none;max-width:380px'
  document.body.appendChild(el)
  stackContainer = el
  return el
}

function mountToastNode(
  kind: ToastKind,
  durationMs: number,
  extraCss: string[] = [],
  setup?: (node: HTMLDivElement, dismiss: () => boolean) => void,
): void {
  const root = ensureContainer()
  if (!root) return
  const palette = PALETTE[kind]
  const node = document.createElement('div')
  node.setAttribute('role', kind === 'error' || kind === 'warn' ? 'alert' : 'status')
  node.setAttribute('aria-live', kind === 'error' || kind === 'warn' ? 'assertive' : 'polite')
  node.style.cssText = [
    `background:${palette.bg}`, `color:${palette.fg}`, `border:1px solid ${palette.border}`,
    'padding:10px 14px', 'border-radius:8px', 'font:13px/1.45 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif',
    'box-shadow:0 8px 24px rgba(0,0,0,.18)', 'pointer-events:auto', 'transition:opacity .18s ease,transform .18s ease',
    'opacity:0', 'transform:translateY(-4px)', ...extraCss,
  ].join(';')

  let dismissed = false
  const dismiss = (): boolean => {
    if (dismissed) return false
    dismissed = true
    node.style.opacity = '0'
    node.style.transform = 'translateY(-4px)'
    setTimeout(() => {
      node.remove()
      if (root.childElementCount === 0 && stackContainer) {
        stackContainer.remove()
        stackContainer = null
      }
    }, 200)
    return true
  }

  setup?.(node, dismiss)
  root.appendChild(node)
  requestAnimationFrame(() => {
    node.style.opacity = '1'
    node.style.transform = 'translateY(0)'
  })
  setTimeout(dismiss, durationMs)
}

export function showToast(message: string, kind: ToastKind = 'info', opts: ToastOptions = {}): void {
  if (typeof document === 'undefined') return
  const now = Date.now()
  if (now - (recentMessages.get(message) || 0) < RECENT_DEDUP_MS) return
  recentMessages.set(message, now)
  if (recentMessages.size > 32) {
    for (const [k, t] of recentMessages) {
      if (now - t > RECENT_DEDUP_MS * 4) recentMessages.delete(k)
    }
  }
  const duration = opts.durationMs ?? (kind === 'error' ? 5500 : 3500)
  mountToastNode(kind, duration, [], (node, dismiss) => {
    node.textContent = message
    node.addEventListener('click', dismiss)
  })
}

export function showReadOnlyToast(): void {
  showToast('Read-only access — that action is unavailable for shared sessions.', 'warn')
}

export function showBusyToast(): void {
  showToast('Server busy, retrying…', 'info', { durationMs: 2500 })
}

export function showToastWithAction(message: string, opts: ToastActionOptions): void {
  if (typeof document === 'undefined') return
  const kind: ToastKind = opts.kind ?? 'info'
  const palette = PALETTE[kind]
  mountToastNode(kind, opts.durationMs ?? 5500, ['display:flex', 'align-items:center', 'gap:12px'], (node, dismiss) => {
    const label = document.createElement('span')
    label.textContent = message
    label.style.cssText = 'flex:1;min-width:0'
    node.appendChild(label)

    const button = document.createElement('button')
    button.type = 'button'
    button.textContent = opts.actionLabel
    button.style.cssText = `background:transparent;color:${palette.fg};border:1px solid ${palette.fg};border-radius:6px;padding:4px 10px;font:600 12px/1 inherit;cursor:pointer;flex-shrink:0;opacity:.9`
    button.addEventListener('click', (e) => {
      e.stopPropagation()
      if (dismiss()) opts.onAction()
    })
    node.appendChild(button)
  })
}

// ---------------------------------------------------------------------------
// Time-range wire contract & anchor quantization (folded from range-wire.ts)
// ---------------------------------------------------------------------------

export const DEFAULT_RANGE_TOKEN = '24h'

export interface RangeWireArgs {
  relativeRange: string | null
  isAutoRange: boolean
  startTime: string | null
  endTime: string | null
  anchor: string
}

export interface RangeWire {
  rangeToken: string | null
  rangeKey: string
  rangeBody:
    | { range_token: string; anchor: string; start_time: string | null; end_time: string | null }
    | { start_time: string | null; end_time: string | null }
}

export function resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor }: RangeWireArgs): RangeWire {
  const rangeToken = relativeRange ?? (isAutoRange ? DEFAULT_RANGE_TOKEN : null)
  if (rangeToken !== null) {
    return {
      rangeToken,
      rangeKey: rangeToken,
      rangeBody: { range_token: rangeToken, anchor, start_time: startTime, end_time: endTime },
    }
  }
  return {
    rangeToken: null,
    rangeKey: `abs:${startTime ?? ''}|${endTime ?? ''}`,
    rangeBody: { start_time: startTime, end_time: endTime },
  }
}

export const ANCHOR_QUANTUM_SECONDS = 60

function isoZ(ms: number): string {
  return new Date(ms).toISOString().replace(/\.\d{3}Z$/, 'Z')
}

export function quantizeAnchor(iso?: string | null, now: Date = new Date()): string {
  const parsed = iso ? Date.parse(iso) : NaN
  const ms = Number.isFinite(parsed) ? parsed : now.getTime()
  const epoch = Math.floor(ms / 1000)
  return isoZ((epoch - (epoch % ANCHOR_QUANTUM_SECONDS)) * 1000)
}

// ---------------------------------------------------------------------------
// URL Filter Hydration (folded from urlFilterHydration.ts)
// ---------------------------------------------------------------------------

let hydrated = false

export function rangeLabelToHours(label: string): number | null {
  const m = /^(\d+)([mhd])$/.exec(label)
  if (!m) return null
  const n = parseInt(m[1], 10)
  if (!Number.isFinite(n) || n <= 0) return null
  return m[2] === 'm' ? n / 60 : m[2] === 'd' ? n * 24 : n
}

export function hydrateFilterStoreFromUrl(): void {
  if (hydrated || typeof window === 'undefined') return
  hydrated = true

  const params = new URLSearchParams(window.location.search)
  const qsRange = params.get('range')
  const qsStart = params.get('start_time')
  const qsEnd = params.get('end_time')
  const qsFilters = params.get('filters')
  const store = useFilterStore.getState()

  const rangeHours = qsRange ? rangeLabelToHours(qsRange) : null
  let touchedRange = false
  if (qsRange && rangeHours !== null) {
    const now = new Date()
    store.setRelativeRange(qsRange, new Date(now.getTime() - rangeHours * 3600 * 1000).toISOString(), now.toISOString())
    touchedRange = true
  } else if (qsStart && qsEnd) {
    store.setRange(qsStart, qsEnd)
    touchedRange = true
  }

  let touchedFilters = false
  if (qsFilters) {
    try {
      const parsed = JSON.parse(qsFilters) as FiltersPayload
      if (parsed && typeof parsed === 'object') {
        store.clearFilters()
        touchedFilters = true
        for (const [rawCol, spec] of Object.entries(parsed)) {
          if (!spec || !Array.isArray(spec.values)) continue
          const col = rawCol.replace(/_\d+$/, '')
          for (const v of spec.values) store.addFilter(col, String(v), spec.mode === 'exclude' ? 'exclude' : 'include')
        }
      }
    } catch {
      // Malformed ?filters= — ignore silently rather than break the page.
    }
  }

  if (!touchedFilters) {
    let cleared = false
    params.forEach((value, key) => {
      if (!key.startsWith('filter_')) return
      if (!cleared) {
        store.clearFilters()
        cleared = true
        touchedFilters = true
      }
      store.addFilter(key.substring('filter_'.length).replace(/_\d+$/, ''), value, 'include')
    })
  }

  if (touchedRange || touchedFilters) {
    const url = new URL(window.location.href)
    for (const k of ['range', 'start_time', 'end_time', 'filters']) url.searchParams.delete(k)
    Array.from(url.searchParams.keys()).forEach((key) => {
      if (key.startsWith('filter_')) url.searchParams.delete(key)
    })
    window.history.replaceState({}, '', url.toString())
  }
}

export function _resetUrlHydrationFlag(): void {
  hydrated = false
}

// ---------------------------------------------------------------------------
// Anomaly Detection (/insights) thematic sectioning (folded from insight-sections.ts)
// ---------------------------------------------------------------------------

interface InsightSection {
  /** Stable machine key. Matches the backend `category` enum value. */
  key: string
  label: string
  description: string
  icon: LucideIcon
}

const ALL_SECTION: InsightSection = {
  key: 'all',
  label: 'All Categories',
  description: 'All automated insights and detected anomalies across every category.',
  icon: LayoutGrid,
}

export const INSIGHT_SECTIONS: readonly InsightSection[] = [
  { key: 'security', label: 'Security & Threat Detection', description: 'Suspicious signatures, scanning, spoofing, proxy/abuse, and WAF activity.', icon: ShieldAlert },
  { key: 'origin', label: 'Origin Health & Stability', description: 'Origin 5xx, latency/timeouts, retries, per-IP failures, and the shield path.', icon: ServerCrash },
  { key: 'edge', label: 'Edge & Delivery Performance', description: 'Cache efficiency, edge/regional latency and tail, and delivery quality.', icon: Zap },
  { key: 'network', label: 'Network & Routing Path', description: 'ISP / ASN reachability, RTT and packet loss, and metro/region delivery quality.', icon: Network },
  { key: 'traffic', label: 'Traffic & Volumetrics', description: 'Volume swings, geography, composition, and automation.', icon: Globe },
] as const

export const SECTION_LOADING_MESSAGES: Record<string, string> = {
  all: 'Evaluating detector baseline comparisons…',
  security: 'Analyzing security signatures & JA3 patterns…',
  origin: 'Evaluating origin 5xx errors & latency shifts…',
  edge: 'Measuring cache collapse & delivery performance…',
  network: 'Checking ISP latency & routing packet loss…',
  traffic: 'Scanning traffic surges & botnet clustering…',
  other: 'Evaluating anomaly detector results…',
}

export function normalizeTabKey(tab: string | null | undefined): string {
  if (!tab) return 'all'
  const lower = tab.toLowerCase().trim()
  return lower === 'performance' ? 'edge' : lower === 'routing' ? 'network' : lower
}

export const OTHER_SECTION: InsightSection = {
  key: 'other', label: 'Other', description: 'Insights not yet assigned to a section.', icon: LayoutGrid,
}

const SECTION_BY_KEY = new Map<string, InsightSection>(INSIGHT_SECTIONS.map((s) => [s.key, s]))

export function categoryToSection(category: string | null | undefined): InsightSection {
  return (category && SECTION_BY_KEY.get(category)) || OTHER_SECTION
}

const SEVERITY_ORDER = ['critical', 'error', 'warning', 'info', 'clean'] as const
const SEVERITY_RANK: Record<string, number> = Object.fromEntries(SEVERITY_ORDER.map((sev, i) => [sev, i]))

export interface SectionableInsight {
  category?: string | null
  severity?: string | null
  title?: string | null
}

export function sortInsights<T extends SectionableInsight>(cards: readonly T[]): T[] {
  return [...cards].sort((a, b) => {
    const ra = SEVERITY_RANK[a.severity ?? ''] ?? 99
    const rb = SEVERITY_RANK[b.severity ?? ''] ?? 99
    return ra !== rb ? ra - rb : (a.title ?? '').localeCompare(b.title ?? '')
  })
}

export interface SeveritySummary {
  severity: string
  count: number
}

export function summarizeSeverities(cards: readonly SectionableInsight[]): SeveritySummary[] {
  const counts = new Map<string, number>()
  for (const card of cards) {
    if (card.severity && card.severity in SEVERITY_RANK) counts.set(card.severity, (counts.get(card.severity) ?? 0) + 1)
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

export function groupInsightsBySection<T extends SectionableInsight>(
  cards: readonly T[],
  options?: { includeAll?: boolean; includeEmptySections?: boolean },
): InsightSectionGroup<T>[] {
  const buckets = new Map<string, T[]>()
  for (const card of cards) {
    const key = categoryToSection(card.category).key
    const bucket = buckets.get(key)
    if (bucket) bucket.push(card)
    else buckets.set(key, [card])
  }

  const groups: InsightSectionGroup<T>[] = options?.includeAll
    ? [{ section: ALL_SECTION, cards: sortInsights(cards) }]
    : []
  for (const section of [...INSIGHT_SECTIONS, OTHER_SECTION]) {
    const bucket = buckets.get(section.key)
    if (bucket?.length) groups.push({ section, cards: sortInsights(bucket) })
    else if (options?.includeEmptySections && section.key !== OTHER_SECTION.key) groups.push({ section, cards: [] })
  }
  return groups
}

// ---------------------------------------------------------------------------
// Traffic Chart Trace Builder (folded from traffic-chart.ts)
// ---------------------------------------------------------------------------

type Aggregates = Partial<Pick<components['schemas']['AggregatesResponse'], 'metric' | 'interval' | 'time_series'>>
type Catalog = {
  fields?: Pick<components['schemas']['LogFieldCatalogEntry'], 'id' | 'label' | 'unit' | 'precision'>[] | null
}

export interface BuildTrafficDataParams {
  aggregates: Aggregates | null | undefined
  compareAggregates: Aggregates | null | undefined
  compareMode: boolean
  compareStartTime: string | null | undefined
  startTime: string | null
  endTime: string | null
  trend: string
  timezone: string
  metric: string
  effectiveInterval: string
  hiddenCategories: ReadonlySet<string>
  catalog: Catalog | null | undefined
}

export function getIntervalSeconds(intervalStr?: string | null): number | undefined {
  if (!intervalStr) return undefined
  const str = intervalStr.trim().toLowerCase()
  if (INTERVAL_SECONDS[str as keyof typeof INTERVAL_SECONDS] !== undefined) {
    return INTERVAL_SECONDS[str as keyof typeof INTERVAL_SECONDS]
  }
  const match = str.match(/^(?:(\d+)\s*)?(s|sec|second|seconds|m|min|minute|minutes|h|hr|hour|hours|d|day|days)$/)
  if (match) {
    const num = match[1] ? parseInt(match[1], 10) : 1
    const u = match[2][0]
    return u === 's' ? num : u === 'm' ? num * 60 : u === 'h' ? num * 3600 : num * 86400
  }
  return undefined
}

export function buildTrafficData({
  aggregates, compareAggregates, compareMode, compareStartTime, startTime, endTime,
  trend, timezone, metric, effectiveInterval, hiddenCategories, catalog,
}: BuildTrafficDataParams): PlotlyTrace[] {
  const time_series = aggregates?.time_series
  if (!time_series?.length) return []

  const actualMetric = aggregates?.metric || metric
  const isBar = actualMetric === 'requests' || actualMetric === '5xx' || actualMetric === '4xx'
  const metricField = catalog?.fields?.find((f) => f.id === actualMetric)
  const unit = metricField?.unit || ''
  const precision = metricField?.precision ?? (actualMetric === 'requests' ? 0 : 1)

  const getHoverTemplate = (_m: string, label?: string) => {
    const pre = label ? `${label}: ` : ''
    const format = precision > 0 ? `.${precision}f` : ','
    return `${pre}%{y:${format}}${unit}<extra></extra>`
  }

  const hasCategories = time_series.some((d) => d.category != null)
  const actualInterval = aggregates?.interval || effectiveInterval
  const intervalSeconds = getIntervalSeconds(actualInterval)
  const barSeries: BarSeriesPoint[] = isBar
    ? densifyBarSeries(time_series, intervalSeconds, hasCategories, startTime, endTime)
    : time_series

  let tsXValues: string[] | null = null
  const getTsXValues = (): string[] => (tsXValues ??= time_series.map((d) => formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss")))

  let traces: PlotlyTrace[] = []
  if (hasCategories) {
    const catMap: Record<string, { x: string[], y: number[] }> = {}
    barSeries.forEach((d) => {
      const cat = d.category || 'Other'
      ;(catMap[cat] ??= { x: [], y: [] }).x.push(formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
      catMap[cat].y.push(d.value)
    })
    const colorMap: Record<string, string> = {
      '400': '#fbbf24', '401': CHART_COLORS.warning, '403': '#d97706', '404': '#b45309',
      '500': CHART_COLORS.danger, '502': '#dc2626', '503': '#b91c1c', '504': '#991b1b'
    }
    traces = Object.entries(catMap).map(([cat, data], i) => ({
      x: data.x, y: data.y, type: 'bar', width: intervalSeconds ? intervalSeconds * 1000 * 0.9 : undefined,
      name: cat, showlegend: false, visible: hiddenCategories.has(cat) ? 'legendonly' : true,
      hovertemplate: `Status ${cat}: %{y:,}<extra></extra>`,
      marker: { color: colorMap[cat] || `hsl(${(i * 50) % 360}, 70%, 50%)` }
    }))
  } else {
    const xValues = barSeries === time_series ? getTsXValues() : barSeries.map((d) => formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
    traces = [{
      x: xValues, y: barSeries.map((d) => d.value),
      type: isBar ? 'bar' : 'scatter', width: isBar && intervalSeconds ? intervalSeconds * 1000 * 0.9 : undefined,
      mode: isBar ? undefined : 'lines+markers',
      name: compareMode ? 'Primary Range' : (metricField?.label || actualMetric),
      showlegend: compareMode,
      hovertemplate: getHoverTemplate(actualMetric, compareMode ? 'Primary' : undefined),
      marker: { color: CHART_COLORS.info }
    }]
  }

  if (compareMode && compareAggregates?.time_series?.length && !hasCategories && startTime && compareStartTime) {
    const shift = new Date(startTime).getTime() - new Date(compareStartTime).getTime()
    traces.push({
      x: compareAggregates.time_series.map((d) => formatDate(new Date(toUTCDate(d.time).getTime() + shift).toISOString(), timezone, "yyyy-MM-dd HH:mm:ss")),
      y: compareAggregates.time_series.map((d) => d.value),
      type: 'scatter', mode: 'lines', name: 'Comparison Range',
      line: { color: '#f97316', dash: 'dash', width: 2 },
      hovertemplate: getHoverTemplate(actualMetric, 'Comparison')
    })
  }

  if (!hasCategories && time_series.some((d) => d.baseline != null)) {
    traces.push({
      x: getTsXValues(), y: time_series.map((d) => d.baseline),
      type: 'scatter', mode: 'lines', name: 'Baseline (7d prior)',
      hovertemplate: getHoverTemplate(actualMetric, 'Baseline'),
      line: { color: '#a1a1aa', dash: 'dot', width: 2 }
    })
  }

  if (!hasCategories && trend !== 'off') {
    const xValues = getTsXValues()
    const yValues = time_series.map((d) => d.value)
    const n = yValues.length
    let windowSize = 0
    if (trend === 'auto') {
      windowSize = Math.floor(n / (n > 1000 ? 20 : n > 100 ? 10 : 5))
    } else {
      const trendMap: Record<string, number> = { '1m': 60, '5m': 300, '1h': 3600, '1d': 86400 }
      const sec = getIntervalSeconds(actualInterval) ?? 60
      windowSize = Math.floor((trendMap[trend] ?? 0) / sec)
    }
    if (windowSize > 1) {
      const trendY: (number | null)[] = new Array(n).fill(null)
      if (n >= windowSize) {
        let runningSum = 0, runningCount = 0
        for (let i = 0; i < n; i++) {
          const enterVal = yValues[i]
          if (enterVal != null) { runningSum += enterVal; runningCount++ }
          if (i >= windowSize) {
            const exitVal = yValues[i - windowSize]
            if (exitVal != null) { runningSum -= exitVal; runningCount-- }
          }
          if (i >= windowSize - 1) trendY[i] = runningCount > 0 ? runningSum / runningCount : null
        }
      }
      traces.push({
        x: xValues, y: trendY, type: 'scatter', mode: 'lines',
        name: `${trend === 'auto' ? 'Auto ' : ''}Trend`,
        hovertemplate: getHoverTemplate(actualMetric),
        line: { color: '#f97316', width: 3 }
      })
    }
  }
  return traces
}
