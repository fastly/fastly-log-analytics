import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"
import { formatDate } from '@/lib/date'


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
