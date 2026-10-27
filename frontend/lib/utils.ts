import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'
import { ShieldAlert, ServerCrash, Zap, Network, Globe, LayoutGrid, type LucideIcon } from 'lucide-react'

export * from './format'
export * from './colors'
export * from './chartUtils'
export * from './sqlBuilder'
export * from './urlFilter'
export * from './exportCsv'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function keyByOccurrence<T>(items: readonly T[], label: (item: T) => string): { key: string; item: T }[] {
  const seen = new Map<string, number>()
  return items.map((item) => {
    const base = label(item)
    const n = seen.get(base) ?? 0
    seen.set(base, n + 1)
    return { key: n === 0 ? base : `${base}#${n}`, item }
  })
}

export function recordOrNull<T extends object>(v: T | null | undefined): T | null {
  return typeof v === 'object' && v !== null && !Array.isArray(v) ? (v as T) : null
}

export function arrayOrEmpty<T>(v: readonly T[] | null | undefined): T[] {
  return Array.isArray(v) ? [...v] : []
}

export function oneOf<T extends string>(allowed: readonly T[], value: string | null | undefined): T | undefined {
  return allowed.find((a) => a === value)
}

export const IP_FAMILY_FIELDS = ['ip', 'client_ip', 'ip_address', 'remote_addr'] as const

export function isIpFamilyField(column: string): boolean {
  return (IP_FAMILY_FIELDS as readonly string[]).includes(column)
}

export const ACTIVE_SERVICE_COOKIE = 'fla.activeServiceId'
export const DEBUG_RESPONSES_COOKIE = 'fla.debugResponses'
export const SIDEBAR_COLLAPSED_COOKIE = 'fla.sidebarCollapsed'

export function setActiveServiceCookie(serviceId: string | null) {
  if (typeof document === 'undefined') return
  const valid = serviceId && /^[A-Za-z0-9]{10,30}$/.test(serviceId)
  document.cookie = `${ACTIVE_SERVICE_COOKIE}=${valid ? serviceId : ''}; path=/; max-age=${valid ? 31536000 : 0}; samesite=lax`
}

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
  lastThrottledBump = lastInteractionAt = now
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
  return (stackContainer = el)
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
  const isAlert = kind === 'error' || kind === 'warn'
  const node = document.createElement('div')
  node.setAttribute('role', isAlert ? 'alert' : 'status')
  node.setAttribute('aria-live', isAlert ? 'assertive' : 'polite')
  node.style.cssText = [
    `background:${palette.bg};color:${palette.fg};border:1px solid ${palette.border}`,
    'padding:10px 14px;border-radius:8px;font:13px/1.45 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif',
    'box-shadow:0 8px 24px rgba(0,0,0,.18);pointer-events:auto;transition:opacity .18s ease,transform .18s ease;opacity:0;transform:translateY(-4px)',
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
  mountToastNode(kind, opts.durationMs ?? (kind === 'error' ? 5500 : 3500), [], (node, dismiss) => {
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
  mountToastNode(kind, opts.durationMs ?? 5500, ['display:flex;align-items:center;gap:12px'], (node, dismiss) => {
    const label = Object.assign(document.createElement('span'), { textContent: message })
    label.style.cssText = 'flex:1;min-width:0'
    const button = Object.assign(document.createElement('button'), { type: 'button', textContent: opts.actionLabel })
    button.style.cssText = `background:transparent;color:${palette.fg};border:1px solid ${palette.fg};border-radius:6px;padding:4px 10px;font:600 12px/1 inherit;cursor:pointer;flex-shrink:0;opacity:.9`
    button.addEventListener('click', (e) => {
      e.stopPropagation()
      if (dismiss()) opts.onAction()
    })
    node.append(label, button)
  })
}

export interface InsightSection {
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
    label: 'Network & Routing Path',
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

export const SECTION_LOADING_MESSAGES: Record<string, string> = {
  all: 'Evaluating detector baseline comparisons…',
  security: 'Analyzing security signatures & JA3 patterns…',
  origin: 'Evaluating origin 5xx errors & latency shifts…',
  edge: 'Measuring cache collapse & delivery performance…',
  network: 'Checking ISP latency & routing packet loss…',
  traffic: 'Scanning traffic surges & botnet clustering…',
  other: 'Evaluating anomaly detector results…',
}

export const OTHER_SECTION: InsightSection = {
  key: 'other',
  label: 'Other',
  description: 'Insights not yet assigned to a section.',
  icon: LayoutGrid,
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
    if (card.severity && card.severity in SEVERITY_RANK) {
      counts.set(card.severity, (counts.get(card.severity) ?? 0) + 1)
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
