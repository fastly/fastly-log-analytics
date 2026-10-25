import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"
import { formatDate } from '@/lib/date'
import { useFilterStore } from '@/stores/filterStore'
import type { FiltersPayload } from '@/types/filters'


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
  '1 second': 1,
  '1 minute': 60,
  '1 hour': 3600,
  '1 day': 86400,
}

export const TRENDS = [
  { label: 'Off', value: 'off' },
  { label: 'Auto', value: 'auto' },
  { label: '1m', value: '1m' },
  { label: '5m', value: '5m' },
  { label: '1h', value: '1h' },
  { label: '1d', value: '1d' },
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
  "us-east-1": "iad-va-us",
  "us-west": "bfi-wa-us",
  "us-central-1": "chi-il-us",
  "eu-central-1": "frankfurt-de",
  "eu-south-1": "mxp-milan-it",
  "eu-west-1": "mxp-paris-fr",
  "uk-east-1": "london-uk",
  "jp-central-1": "nrt-tokyo-jp",
  "au-east-1": "sydney-au",
};

export const REGION_LABELS: Record<string, string> = {
  "us-east-1": "US East (Ashburn)",
  "us-west": "US West (Seattle)",
  "us-central-1": "US Central (Chicago)",
  "eu-central-1": "EU Central (Frankfurt)",
  "eu-south-1": "EU South (Milan)",
  "eu-west-1": "EU West (Paris)",
  "uk-east-1": "UK East (London)",
  "jp-central-1": "JP Central (Tokyo)",
  "au-east-1": "AU East (Sydney)",
};

export const SHIELD_LABELS: Record<string, string> = {
  none: "None",
  "iad-va-us": "IAD (Ashburn)",
  "bfi-wa-us": "BFI (Seattle)",
  "chi-il-us": "CHI (Chicago)",
  "frankfurt-de": "FRA (Frankfurt)",
  "mxp-milan-it": "MXP (Milan)",
  "mxp-paris-fr": "MXP (Paris)",
  "london-uk": "LHR (London)",
  "nrt-tokyo-jp": "NRT (Tokyo)",
  "sydney-au": "SYD (Sydney)",
};

export const PERIOD_LABELS: Record<string, string> = {
  "1": "1 second",
  "5": "5 seconds",
  "10": "10 seconds",
  "20": "20 seconds",
  "30": "30 seconds",
  "60": "1 minute",
  "120": "2 minutes",
  "300": "5 minutes",
  "600": "10 minutes",
  "900": "15 minutes",
};

export const RETENTION_OPTIONS = [
  { value: "1", label: "1 day" },
  { value: "3", label: "3 days" },
  { value: "7", label: "7 days" },
  { value: "14", label: "14 days" },
  { value: "30", label: "30 days" },
  { value: "90", label: "90 days" },
  { value: "180", label: "180 days" },
  { value: "365", label: "365 days" },
  { value: "0", label: "Forever" },
] as const;

// ---------------------------------------------------------------------------
// Dependency-free DOM toast helper (folded from toast.ts)
// ---------------------------------------------------------------------------

type ToastKind = 'info' | 'success' | 'error' | 'warn'

interface ToastOptions {
  durationMs?: number
}

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
  el.style.cssText =
    'position:fixed;top:16px;right:16px;z-index:2147483647;display:flex;flex-direction:column;gap:8px;pointer-events:none;max-width:380px'
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
    `background:${palette.bg}`,
    `color:${palette.fg}`,
    `border:1px solid ${palette.border}`,
    'padding:10px 14px',
    'border-radius:8px',
    'font:13px/1.45 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif',
    'box-shadow:0 8px 24px rgba(0,0,0,.18)',
    'pointer-events:auto',
    'transition:opacity .18s ease,transform .18s ease',
    'opacity:0',
    'transform:translateY(-4px)',
    ...extraCss,
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

  mountToastNode(
    kind,
    opts.durationMs ?? 5500,
    ['display:flex', 'align-items:center', 'gap:12px'],
    (node, dismiss) => {
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
    },
  )
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

export function resolveRangeWire({
  relativeRange,
  isAutoRange,
  startTime,
  endTime,
  anchor,
}: RangeWireArgs): RangeWire {
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
  let ms: number
  if (iso) {
    const parsed = Date.parse(iso)
    ms = Number.isFinite(parsed) ? parsed : now.getTime()
  } else {
    ms = now.getTime()
  }
  const epoch = Math.floor(ms / 1000)
  const floored = epoch - (epoch % ANCHOR_QUANTUM_SECONDS)
  return isoZ(floored * 1000)
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
  if (m[2] === 'm') return n / 60
  return m[2] === 'd' ? n * 24 : n
}

export function hydrateFilterStoreFromUrl(): void {
  if (hydrated) return
  if (typeof window === 'undefined') return
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
    const start = new Date(now.getTime() - rangeHours * 3600 * 1000).toISOString()
    store.setRelativeRange(qsRange, start, now.toISOString())
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
          for (const v of spec.values) {
            store.addFilter(col, String(v), spec.mode === 'exclude' ? 'exclude' : 'include')
          }
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
      const col = key.substring('filter_'.length).replace(/_\d+$/, '')
      store.addFilter(col, value, 'include')
    })
  }

  if (touchedRange || touchedFilters) {
    const url = new URL(window.location.href)
    url.searchParams.delete('range')
    url.searchParams.delete('start_time')
    url.searchParams.delete('end_time')
    url.searchParams.delete('filters')
    Array.from(url.searchParams.keys()).forEach((key) => {
      if (key.startsWith('filter_')) url.searchParams.delete(key)
    })
    window.history.replaceState({}, '', url.toString())
  }
}

export function _resetUrlHydrationFlag(): void {
  hydrated = false
}
