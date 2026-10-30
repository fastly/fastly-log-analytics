'use client'

import * as React from 'react'
import { useEffect, useState } from 'react'
import { HydrationBoundary, QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { DehydratedState } from '@tanstack/react-query'
import dynamic from 'next/dynamic'
import { usePathname } from 'next/navigation'
import { useReportWebVitals } from 'next/web-vitals'
import { NuqsAdapter } from 'nuqs/adapters/next/app'
import { hydrateFilterStoreFromUrl } from '@/lib/utils'
import { getApiBase } from '@/lib/api'
import { useBootstrap } from '@/hooks/useBootstrap'
import { useServiceStore } from '@/stores/filterStore'
import { useTimezoneStore } from '@/stores/filterStore'
import { useDebugStore } from '@/stores/filterStore'
import { setActiveServiceCookie } from '@/lib/utils'

// Reload-loop breaker (defense-in-depth).
//
// A stale SPA tab left open across a deploy can, in rare cases, fall into a
// hard-reload loop: Next.js's deployment-skew / chunk-load recovery keeps
// calling window.location.reload() to re-sync the client to the new build.
// The `private, no-cache` HTML header (next.config.ts) normally makes that
// self-heal in a single reload, but this is belt-and-suspenders for any case
// where it does NOT (a real occurrence flooded RUM with ~9.5k samples over
// hours).
//
// How it breaks the loop: it counts hard document loads of the SAME path in a
// short window (a loop hammers one URL; ordinary client-side navigation across
// pages doesn't trigger a document load, and address-bar / harness navigation
// lands on different paths). Once the count crosses the threshold it renders a
// manual-recovery prompt INSTEAD of the app subtree — not mounting the app is
// what actually stops the loop, since the unmounted tree no longer prefetches
// routes or runs the navigation that makes Next reload.
//
// Implemented with useSyncExternalStore so it's SSR/hydration-safe
// (getServerSnapshot → false, so server and client-initial render both produce
// children) and free of the setState-in-effect / ref-in-render foot-guns.

const STORAGE_KEY = '__reload_loop_history'
const WINDOW_MS = 60_000
// Same-path hard loads within WINDOW_MS before we treat it as a loop. A 6–11s
// reload cadence reaches this in ~40–65s; a human almost never hard-reloads one
// page 6× in a minute, and the fallback is dismissible if they do.
const THRESHOLD = 6

type Entry = { t: number; path: string }

function readHistory(): Entry[] {
  try {
    const parsed = JSON.parse(sessionStorage.getItem(STORAGE_KEY) ?? '[]')
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

function clearHistory() {
  try {
    sessionStorage.removeItem(STORAGE_KEY)
  } catch {
    /* sessionStorage unavailable (private mode / disabled) — nothing to clear */
  }
}

// Record one document load. Called from subscribe() (once per mount); the
// snapshot below re-reads sessionStorage after subscription, so the freshly
// recorded load is reflected without a manual notification.
function recordLoad() {
  try {
    const now = Date.now()
    const path = window.location.pathname
    const recent = readHistory().filter((e) => now - e.t < WINDOW_MS)
    recent.push({ t: now, path })
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(recent))
  } catch {
    /* can't persist — detection simply won't trip */
  }
}

function subscribe() {
  recordLoad()
  return () => {}
}

// Pure read: is the current path over the same-path load threshold?
function getClientSnapshot(): boolean {
  try {
    const now = Date.now()
    const path = window.location.pathname
    const recent = readHistory().filter((e) => now - e.t < WINDOW_MS)
    return recent.filter((e) => e.path === path).length >= THRESHOLD
  } catch {
    return false
  }
}

function getServerSnapshot(): boolean {
  return false
}

export function ReloadLoopGuard({ children }: { children: React.ReactNode }) {
  const tripped = React.useSyncExternalStore(subscribe, getClientSnapshot, getServerSnapshot)
  const [dismissed, setDismissed] = React.useState(false)

  if (tripped && !dismissed) {
    return (
      <ReloadLoopFallback
        onRefresh={() => {
          clearHistory()
          window.location.reload()
        }}
        onContinue={() => {
          clearHistory()
          setDismissed(true)
        }}
      />
    )
  }

  return <>{children}</>
}

function ReloadLoopFallback({
  onRefresh,
  onContinue,
}: {
  onRefresh: () => void
  onContinue: () => void
}) {
  return (
    <div role="alert" aria-live="assertive" className="flex min-h-screen items-center justify-center p-6">
      <div className="flex w-full max-w-md flex-col items-start gap-3 rounded-lg border border-amber-500/30 bg-amber-500/10 p-6">
        <h1 className="text-base font-semibold text-foreground">This page kept reloading</h1>
        <p className="text-sm text-muted-foreground">
          It reloaded several times in a row — usually because a new version was
          deployed while this tab was open. Refresh to load the latest version.
        </p>
        <div className="flex gap-2">
          <button
            type="button"
            onClick={onRefresh}
            className="rounded-md bg-primary px-3 py-1.5 text-sm font-medium text-primary-foreground hover:opacity-90"
          >
            Refresh now
          </button>
          <button
            type="button"
            onClick={onContinue}
            className="rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium hover:bg-accent hover:text-accent-foreground"
          >
            Continue anyway
          </button>
        </div>
      </div>
    </div>
  )
}

// Rehydrate the persisted Zustand stores AFTER mount.
//
// All three stores use persist({ skipHydration: true }), so their first
// client render uses the same default state the server rendered with
// (activeServiceId=null, services=[], timezone='UTC', debug=false).
// Without skipHydration, persist reads localStorage synchronously at
// module load, so the first client render diverges from the server (no
// localStorage there) and throws React #418 across every node that
// depends on the store — most visibly the sidebar nav `?service=` hrefs
// and the ServicesTable "Active" badge.
//
// rehydrate() runs here in an effect (client-only, after the first paint)
// so the persisted values land as an ordinary state update rather than a
// hydration mismatch.
export function StoreHydrator() {
  useEffect(() => {
    useServiceStore.persist.rehydrate()
    useDebugStore.persist.rehydrate()

    // Sync cookie with the rehydrated activeServiceId (Zustand persist bypasses setters)
    const activeServiceId = useServiceStore.getState().activeServiceId
    if (activeServiceId) {
      setActiveServiceCookie(activeServiceId)
    }

    // URL parameter overrides localStorage activeServiceId if present.
    // This fixes the bug where reloading or deep-linking to a page with
    // a service in the query parameter (such as on stream detail or dashboard)
    // would be incorrectly overwritten by the previously active service in localStorage.
    if (typeof window !== 'undefined') {
      const params = new URLSearchParams(window.location.search)
      const urlService = params.get('service')
      if (urlService) {
        useServiceStore.getState().setActiveServiceId(urlService)
      }
    }

    // Timezone: rehydrate the persisted mode/zone, then re-resolve the
    // live system zone on every mount while mode is 'system'. Doing this
    // post-mount means it never contradicts the server's deterministic
    // 'UTC' default. A user who picked an explicit zone (mode: 'manual')
    // keeps exactly that zone — this never overwrites it.
    useTimezoneStore.persist.rehydrate()
    if (useTimezoneStore.getState().mode === 'system') {
      try {
        useTimezoneStore
          .getState()
          .setSystemTimezone(Intl.DateTimeFormat().resolvedOptions().timeZone)
      } catch {
        /* keep whatever's already in state */
      }
    }
  }, [])

  return null
}

/**
 * Wires Next.js's web-vitals callback to POST /api/web-vitals.
 *
 * Each metric (LCP / INP / CLS / FCP / TTFB) fires once per page load
 * (CLS / INP keep updating mid-life; the SDK delivers deltas under the
 * same id). We send via navigator.sendBeacon when available so the
 * request survives a fast unload; the fetch fallback covers Safari
 * and older browsers that throttle sendBeacon during bfcache restore.
 *
 * Backend logs each event via structlog — no SQLite write, no schema
 * migration. Slice by name / rating / pathname / cohort in log
 * aggregation (or wire a dashboard panel later).
 */
export function WebVitalsReporter() {
  const pathname = usePathname()
  const { data: bootstrap } = useBootstrap()

  // Collection is opt-in on the backend (WEB_VITALS_COLLECT), mirrored into
  // bootstrap.settings.web_vitals_enabled. When off we skip the POST entirely
  // so a disabled deployment sees zero web-vitals traffic — no beacons, no
  // access-log lines. Read through a ref so the captured useReportWebVitals
  // callback always sees the latest value: bootstrap resolves a tick after
  // first paint, and the callback is registered once on mount.
  const enabledRef = React.useRef(false)
  const webVitalsEnabled = Boolean(bootstrap?.settings?.web_vitals_enabled)
  React.useEffect(() => {
    enabledRef.current = webVitalsEnabled
  }, [webVitalsEnabled])

  const pathnameRef = React.useRef(pathname)
  React.useEffect(() => {
    pathnameRef.current = pathname
  }, [pathname])

  const disabledRef = React.useRef(false)
  const sentKeysRef = React.useRef<Set<string>>(new Set())

  const handleReportWebVitals = React.useCallback((metric: Parameters<Parameters<typeof useReportWebVitals>[0]>[0]) => {
    if (disabledRef.current) return
    if (!enabledRef.current) return
    const dedupeKey = `${metric.name}:${metric.id}:${metric.value}`
    if (sentKeysRef.current.has(dedupeKey)) return
    sentKeysRef.current.add(dedupeKey)
    const body = {
      id: metric.id,
      name: metric.name,
      value: metric.value,
      rating: metric.rating,
      pathname: pathnameRef.current,
      navigation_type: metric.navigationType,
      delta: metric.delta,
    }
    const json = JSON.stringify(body)
    const url = `${getApiBase()}/api/web-vitals`

    try {
      // sendBeacon is fire-and-forget and survives the unload that
      // hides the page right after the final metric fires.
      if (typeof navigator !== 'undefined' && 'sendBeacon' in navigator) {
        const blob = new Blob([json], { type: 'application/json' })
        const ok = navigator.sendBeacon(url, blob)
        if (ok) return
      }
      // Fallback for browsers that returned false from sendBeacon
      // (quota / unsupported content-type / strict-mode bfcache).
      void fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: json,
        // Keep alive across unload — chrome / firefox honor this; safari
        // best-effort.
        keepalive: true,
      }).then((res) => {
        if (res.status === 401 || res.status === 403) {
          disabledRef.current = true
        }
      })
    } catch {
      // Telemetry is fire-and-forget; never let a failed report break
      // the page that triggered it.
    }
  }, [])

  useReportWebVitals(handleReportWebVitals)

  return null
}

// Run URL→filterStore hydration at MODULE LOAD on the client, before
// any React component renders. The previous useState-initializer
// approach ran inside QueryProvider's first render — same render tick
// as the page's first useFilterStore() call. On routes like /security
// that's a race: the page subscribed to the default 24h startTime,
// fired its first /api/security/aggregates against that key, then the
// hydrate's write-to-store notified the subscriber and a SECOND fetch
// fired against the URL-derived window — visible in HARs as
// hits_per_load = 2 with the first request returning stale data.
// Module-load hydration writes the store before any Zustand subscriber
// is established.
if (typeof window !== 'undefined') {
  hydrateFilterStoreFromUrl()
}

const ReactQueryDevtools = dynamic(
  () => import('@tanstack/react-query-devtools').then(m => ({ default: m.ReactQueryDevtools })),
  { ssr: false }
)

interface QueryProviderProps {
  children: React.ReactNode
  // Optional React Query dehydrated state from a server component
  // (typically app/layout.tsx). When present, the client cache is
  // seeded on first mount so hooks like useBootstrap find data
  // already cached and skip their first network fetch entirely.
  dehydratedState?: DehydratedState | null
}

export default function QueryProvider({ children, dehydratedState }: QueryProviderProps) {

  const [queryClient] = useState(() => new QueryClient({
    defaultOptions: {
      queries: {
        // staleTime: queries stay "fresh" for 60s after fetch. Repeat
        // navigations to a route within 60s skip the network entirely
        // — that's the difference between "click → instant snapshot"
        // vs "click → spinner → repaint" for revisits.
        staleTime: 5 * 60 * 1000,
        // gcTime: keep cached data in memory for 5 min after the last
        // subscriber unmounts. Without this React Query drops the
        // cache the moment a page unmounts, so navigating away and
        // back pays a cold fetch even within seconds. 5 min covers
        // typical click-back behaviour without bloating memory.
        gcTime: 5 * 60 * 1000,
        refetchOnWindowFocus: false,
        // Skip retries on 4xx (caller error — retrying just amplifies
        // the same failure into 3-4x traffic per the React Query
        // default of `retry: 3`). Allow up to 2 retries on 5xx / network
        // errors where a retry can plausibly succeed. The /api/sessions
        // CORS preflight failure used to fan one user click into 4
        // identical /api/sessions POSTs visible in HAR.
        //
        // E-5 (audit): 503 with err.busy = true (DuckDB pool saturation)
        // gets one extra retry (3 total) because the failure mode is
        // explicitly transient — the backend is telling us "try again
        // shortly" rather than "this is broken". Exponential backoff
        // (1s → 2s → 4s, 8s cap) gives the pool room to drain instead
        // of hammering it with three retries in <300ms.
        retry: (failureCount: number, error: unknown) => {
          // NO_SERVICE sentinel from lib/api.ts: retrying re-throws the
          // same sentinel until the activeService store transitions and
          // the queryKey changes (which is what refires the query). Retry
          // is pure waste and visibly extends the "loading" interval on
          // service-switch.
          if ((error as { code?: string } | null)?.code === 'NO_SERVICE') return false
          const status = (error as { response?: { status?: number } } | null)?.response?.status
          if (status !== undefined && status >= 400 && status < 500) return false
          const busy = (error as { busy?: boolean } | null)?.busy === true
          if (busy) return failureCount < 3
          return failureCount < 2
        },
        retryDelay: (attemptIndex: number) =>
          Math.min(1000 * 2 ** attemptIndex, 8000),
      },
      mutations: {
        // Same retry-on-4xx-is-amplification rule for mutations (the
        // /api/sessions POST that powers session list refresh is a
        // mutation, not a query).
        retry: (failureCount: number, error: unknown) => {
          const status = (error as { response?: { status?: number } } | null)?.response?.status
          if (status !== undefined && status >= 400 && status < 500) return false
          return failureCount < 1
        },
      },
    },
  }))

  return (
    <QueryClientProvider client={queryClient}>
      <NuqsAdapter>
        <HydrationBoundary state={dehydratedState}>
          {children}
        </HydrationBoundary>
      </NuqsAdapter>
      <ReactQueryDevtools initialIsOpen={false} />
    </QueryClientProvider>
  )
}
