'use client'

import * as React from 'react'
import Link from 'next/link'
import { usePathname, useSearchParams } from 'next/navigation'
import { 
  LayoutDashboard, 
  BarChart3, 
  Network, 
  Users, 
  Settings, 
  Database,
  Search,
  Activity,
  Menu,
  Sparkles,
  Timer,
  Shield,
  Bell,
  Server
} from 'lucide-react'

import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { ServiceSwitcher } from '@/components/ServiceSwitcher/ServiceSwitcher'
import { TimezoneSwitcher } from '@/components/TimezoneSwitcher/TimezoneSwitcher'
import { ThemeToggle } from '@/components/ThemeToggle/ThemeToggle'
import { FilterBar } from '@/components/FilterBar/FilterBar'
import { ScrollArea } from '@/components/ui/scroll-area'
import { SyncStatusBadge } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { DebugPanel } from '@/components/DebugPanel'
import { PlotlyPrewarm } from '@/components/PlotlyChart/PlotlyPrewarm'
import { MapPrewarm } from '@/components/Map/MapPrewarm'

import { useUrlServiceSync } from '@/hooks/useUrlServiceSync'
import { useBootstrap } from '@/hooks/useBootstrap'
import { useServiceStore } from '@/stores/serviceStore'
import { useRouter } from 'next/navigation'
import packageJson from '../package.json'
import { useShareStatusBanner } from '@/hooks/useShareStatusBanner'
import { useAnalystHeartbeat } from '@/hooks/useAnalystHeartbeat'

// `analystVisible` controls visibility for FOS-sharing analysts (those
// running their own copy of the app locally against the admin's FOS
// bucket). `shareAnalystVisible` overrides for SHARE-INVITED analysts
// (those using a public URL via the share-login flow). When
// shareAnalystVisible is unset it falls back to analystVisible.
//
// Data Management exposes ingestion logs / cron health — useful for an
// analyst who runs their own ingestion, leak-y for an invited viewer.
const SERVICE_NAVIGATION = [
  { name: 'Dashboard', href: '/dashboard', icon: LayoutDashboard, analystVisible: true },
  { name: 'Performance', href: '/performance', icon: Timer, analystVisible: true },
  { name: 'Origin', href: '/origin', icon: Server, analystVisible: true },
  { name: 'Security', href: '/security', icon: Shield, analystVisible: true },
  { name: 'Charts', href: '/charts', icon: BarChart3, analystVisible: true },
  { name: 'Insights', href: '/insights', icon: Sparkles, analystVisible: true },
  { name: 'Network', href: '/network', icon: Network, analystVisible: true },
  { name: 'Sessions', href: '/sessions', icon: Users, analystVisible: true },
  { name: 'Usage & Cost', href: '/usage', icon: Activity, analystVisible: false },
  { name: 'Query', href: '/query', icon: Search, analystVisible: true },
  { name: 'Alerts', href: '/alerts', icon: Bell, analystVisible: false },
  { name: 'Data Management', href: '/logs', icon: Database, analystVisible: true, shareAnalystVisible: false },
]

const SYSTEM_NAVIGATION = [
  { name: 'Admin', href: '/admin', icon: Settings },
]

function UrlServiceSync() {
  useUrlServiceSync()
  return null
}

// Lifts the `?mode=raw` search-param flag into a callback so the parent
// AppLayout can react to it without calling `useSearchParams()` directly.
// `useSearchParams()` requires a Suspense boundary above it for Next.js
// static rendering; isolating it here lets us wrap just this slice in
// <Suspense> rather than every consumer of the layout.
function RawQueryModeProbe({ onChange }: { onChange: (isRaw: boolean) => void }) {
  const searchParams = useSearchParams()
  const isRaw = searchParams.get('mode') === 'raw'
  React.useEffect(() => {
    onChange(isRaw)
  }, [isRaw, onChange])
  return null
}

interface NavLinkProps {
  href: string
  icon: React.ElementType
  name: string
  isActive: boolean
  disabled?: boolean
}

function NavLink({ href, icon: Icon, name, isActive, disabled, activeServiceId, router }: NavLinkProps & { activeServiceId?: string | null; router: ReturnType<typeof useRouter> }) {
  const finalHref = activeServiceId && !href.startsWith('/admin')
    ? `${href}?service=${activeServiceId}`
    : href

  // Viewport-entry prefetch is disabled (prefetch={false}) — with ~12
  // sidebar items, auto-prefetch fires 30-60 RSC requests per page load
  // (37-66 observed, ~2s bandwidth competition). Instead we prefetch on
  // hover: the mouse takes 100-300ms to travel + dwell before clicking,
  // which is enough for Next.js to fetch the loading boundary so the
  // transition feels instant on click.
  const handleMouseEnter = React.useCallback(() => {
    if (!disabled) router.prefetch(finalHref)
  }, [disabled, finalHref, router])

  return (
    <Link
      href={finalHref}
      prefetch={false}
      onMouseEnter={handleMouseEnter}
      aria-disabled={disabled || undefined}
      tabIndex={disabled ? -1 : undefined}
      className={cn(
        "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
        disabled
          ? "text-muted-foreground opacity-50 cursor-not-allowed pointer-events-none"
          : "hover:bg-accent hover:text-accent-foreground",
        !disabled && isActive ? "bg-primary text-primary-foreground shadow-sm" : !disabled ? "text-muted-foreground" : ""
      )}
    >
      <Icon className="h-4 w-4" />
      {name}
    </Link>
  )
}

export function AppLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname()
  const router = useRouter()
  const { data: bootstrapData, isSuccess, isLoading } = useBootstrap()
  // Tracks whether the current /query page is in raw-SQL mode (?mode=raw).
  // Populated by <RawQueryModeProbe> inside the Suspense boundary below
  // so we don't have to call useSearchParams() directly here.
  const [isRawQueryMode, setIsRawQueryMode] = React.useState(false)

  // (Removed) Navigation cancel pattern was here. The intent was to
  // abort the previous route's in-flight polls on route change, but
  // ``cancelQueries({ type: 'active' })`` fires AFTER React mounts the
  // new page and starts ITS queries — so it cancelled the new page's
  // queries too, leaving every page except dashboard with no data.
  // The right way to do this needs per-route query namespacing or a
  // pre-navigation hook. Until then, accept the small in-flight
  // overlap. The 2s → 10s health-snapshot polling rate change is the
  // real lever for backend pressure.

  const activeServiceId = useServiceStore(state => state.activeServiceId)
  const services = useServiceStore(state => state.services)
  const activeService = services.find(s => s.id === activeServiceId)
  const bootstrapSettings = bootstrapData?.settings as Record<string, unknown> | undefined
  const isAnalyst =
    activeService?.accessLevel === 'read_only' ||
    bootstrapSettings?.is_remote_analyst === true
  // Distinguish share-invited analysts (public URL, share-login flow)
  // from FOS-sharing analysts (running their own copy locally). The
  // former see a more restricted nav (no Data Management, no ops info).
  const isShareAnalyst = bootstrapSettings?.is_remote_analyst === true
  const analystEmail = (bootstrapSettings?.analyst_email as string | undefined) || undefined
  const analystName = (bootstrapSettings?.analyst_name as string | undefined) || undefined

  // Sticky banner + share dialog for local admin only. Gated on isSuccess so
  // we don't poll the admin endpoint before bootstrap has classified us —
  // otherwise anonymous remote visitors trigger a transient 401 in the console
  // before the share-login redirect fires.
  const shareBanner = useShareStatusBanner({ enabled: isSuccess && !isAnalyst })

  // Idle-only heartbeat for remote analysts; redirects to /share-login on 401.
  const { disconnected } = useAnalystHeartbeat({ enabled: isAnalyst })

  const visibleNav = SERVICE_NAVIGATION.filter(item => {
    if (isShareAnalyst) {
      // share analysts see a tighter subset; default to analystVisible when
      // an item doesn't have a share-specific override.
      return item.shareAnalystVisible ?? item.analystVisible ?? false
    }
    if (isAnalyst) return item.analystVisible
    return true
  })
  const visibleSystemNav = isAnalyst ? [] : SYSTEM_NAVIGATION

  // Use bootstrap response as authoritative; fall back to persisted store while
  // the fetch is in-flight or if it errors (e.g. backend down after crash).
  // activeServiceId alone is enough to suppress the redirect — it persists
  // from the wizard completion before bootstrap has had a chance to respond.
  const hasServices = isSuccess
    ? !!(bootstrapData?.services?.length)
    : services.length > 0 || !!activeServiceId

  const needsLogin = bootstrapSettings?.needs_login === true

  React.useEffect(() => {
    if (isLoading) return
    // All router.replace() calls in this redirect block are wrapped in
    // startTransition so React paints the current loading.tsx skeleton
    // first instead of stalling on the synchronous URL change.
    // Anonymous remote visitors get redirected to /share-login before any
    // other layout/redirect logic kicks in. Skip while already there.
    if (needsLogin && !pathname.startsWith('/share-login')) {
      React.startTransition(() => router.replace('/share-login'))
      return
    }
    // Analysts can't access admin pages, the Usage & Cost page, the Alerts
    // surface, or the Data Management page. The backend returns 403 on
    // /api/admin/*, /api/usage/*, /api/alerts/*, /api/cron-runs and friends,
    // but the page shells are served by Next.js — bounce them away client-
    // side so the URL isn't reachable (otherwise the page mounts and
    // silently fails its data fetches).
    //
    // 2026-06-10 audit: ``router.replace`` inside ``startTransition`` was
    // observed NOT firing on prod for /alerts and /logs even though the
    // bundle clearly contained the redirect (verified via direct chunk
    // fetch). The first redirect (/admin) DID work — likely because the
    // page.tsx for /alerts and /logs themselves mount expensive client
    // hooks (useQuery against now-403 endpoints) that race with the
    // transition. Use ``window.location.replace`` for these blocking
    // redirects: a full page navigation is cheap (the analyst never
    // reaches the destination's data fetches anyway), it can't be raced
    // by the destination route's own effects, and it preserves browser
    // history correctly.
    const analystBlocked =
      isAnalyst && (pathname.startsWith('/admin') || pathname.startsWith('/usage') || pathname.startsWith('/alerts'))
    const logsBlocked = (isAnalyst || isShareAnalyst) && pathname.startsWith('/logs')
    if (analystBlocked || logsBlocked) {
      const target = activeServiceId ? `/dashboard?service=${activeServiceId}` : '/dashboard'
      if (typeof window !== 'undefined') {
        window.location.replace(target)
      } else {
        React.startTransition(() => router.replace(target))
      }
      return
    }
    // Admin-side wizard redirect — only for local admins.
    if (!isAnalyst && !hasServices && !pathname.startsWith('/admin')) {
      React.startTransition(() => router.replace('/admin'))
    }
  }, [isLoading, hasServices, isAnalyst, isShareAnalyst, needsLogin, pathname, router, activeServiceId])

  // Only preload world.geojson on routes that actually mount a map
  // (dashboard's "Requests by Country" choropleth, /network's choropleth).
  // Previously this was a global <link rel="preload"> in app/layout.tsx,
  // which fired on every page (including /share-login) and wasted ~251KB
  // on routes that never paint a map. React 19 hoists <link> to <head>
  // automatically when rendered from a client component.
  const needsGeoPreload =
    pathname.startsWith('/dashboard') || pathname.startsWith('/network')

  // Hide the global filter bar on pages where it does not apply.
  // /query is a special case: Structured Mode (default) syncs with the
  // FilterBar, so we keep it visible; Raw SQL Mode (?mode=raw) owns its
  // own editor + filters and the global bar would only confuse the
  // SQL the user is hand-writing.
  const isQueryRawMode = pathname.startsWith('/query') && isRawQueryMode
  const hideFilterBar = pathname.startsWith('/admin') || pathname.startsWith('/logs') || isQueryRawMode || pathname.startsWith('/insights') || pathname.startsWith('/alerts') || !hasServices

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-background">
      {needsGeoPreload && (
        <link
          rel="preload"
          href="/geo/world.geojson"
          as="fetch"
          crossOrigin="anonymous"
        />
      )}
      {shareBanner.node}
      <div className="flex flex-1 overflow-hidden min-h-0">
      {isAnalyst && disconnected && (
        <div
          data-testid="connection-interrupted-overlay"
          className="fixed inset-0 z-[100] bg-background/90 backdrop-blur-sm flex items-center justify-center"
        >
          <div className="max-w-sm text-center space-y-3 p-6 rounded-lg border bg-card shadow-lg">
            <h2 className="text-lg font-semibold">Connection interrupted</h2>
            <p className="text-sm text-muted-foreground">
              We can&apos;t reach the dashboard. We&apos;ll keep trying — your view will
              resume automatically when the connection returns.
            </p>
          </div>
        </div>
      )}
      <React.Suspense fallback={null}>
        <UrlServiceSync />
        <RawQueryModeProbe onChange={setIsRawQueryMode} />
      </React.Suspense>
      {/* Cold-init pre-warmers — intentional perf components, not hacks.
          Plotly (~500-1500ms cold parse + first-plot init) and MapLibre
          GL (~500-1200ms parse + WebGL context + first paint) both pay
          their cold init the first time they render with non-empty data.
          Running a 1-pixel invisible render during app mount moves that
          cost onto the page-load wait the user is already absorbing, so
          the dashboard's real chart/map render hits the fast
          react()-update path. Both modules are used across most analytics
          pages, so app-level rendering is intentional. Full per-component
          rationale in PlotlyPrewarm.tsx + MapPrewarm.tsx. */}
      <PlotlyPrewarm />
      <MapPrewarm />
      {/* Desktop Sidebar */}
      <aside className="hidden md:flex w-64 flex-col border-r bg-muted/40">
        <div className="flex h-14 items-center justify-center border-b px-4 py-2 shrink-0">
          <Link 
            href={hasServices ? (activeServiceId ? `/dashboard?service=${activeServiceId}` : "/dashboard") : "/admin"} 
            className="flex flex-col items-center justify-center hover:opacity-80 transition-opacity mt-1"
          >
             <img src="/fastly.svg" alt="Fastly" className="h-5 dark:invert" />
             <span className="text-[11px] font-bold uppercase tracking-widest text-muted-foreground mt-0.5">Log Analytics</span>
          </Link>
        </div>
        <ScrollArea className="flex-1">
          <nav className="grid gap-1 p-2">
            {visibleNav.map((item) => (
              <NavLink
                key={item.href}
                {...item}
                isActive={pathname === item.href}
                disabled={!hasServices}
                activeServiceId={activeServiceId}
                router={router}
              />
            ))}
          </nav>
        </ScrollArea>
        <div className="mt-auto p-2 border-t bg-muted/20">
          <nav className="grid gap-1">
            {visibleSystemNav.map((item) => (
              <NavLink
                key={item.href}
                {...item}
                isActive={pathname === item.href}
                activeServiceId={activeServiceId}
                router={router}
              />
            ))}
          </nav>
          <div className="mt-4 mb-1 text-[10px] text-muted-foreground/50 text-center font-mono select-all">
            v{packageJson.version}
          </div>
          {isAnalyst && (analystEmail || analystName) && (
            <div
              data-testid="analyst-watermark"
              data-analyst-email={analystEmail || ''}
              className="text-[10px] text-muted-foreground/60 text-center mt-1"
            >
              Viewing as <span className="font-medium">{analystName || analystEmail}</span>
            </div>
          )}
        </div>
      </aside>

      {/* Main Content */}
      <div className="flex flex-1 flex-col overflow-hidden">
        <header className="flex h-14 items-center gap-4 border-b bg-muted/40 px-4 shrink-0">
          <ServiceSwitcher />
          <div className="ml-auto flex items-center gap-2">
            <SyncStatusBadge />
            <TimezoneSwitcher />
            <ThemeToggle />
          </div>
        </header>        
        {!hideFilterBar && <FilterBar />}

        <main className="flex-1 overflow-auto p-6">
          {/* Render children IMMEDIATELY on navigation. The previous
              ``isLoading ? <Spinner /> : children`` gate held every
              route hostage to /api/bootstrap, which has no staleTime —
              meaning every click → blank spinner until the bootstrap
              fetch returned (~1s of lag, observed 2026-06-04). With
              the gate removed, Next.js can render each route's
              loading.tsx skeleton immediately on click, then swap in
              the real page when its React Query data lands. The
              !hasServices short-circuit stays so the onboarding
              redirect at lines 163-188 has time to fire without
              flashing a half-loaded page. */}
          {!hasServices && !pathname.startsWith('/admin') && !pathname.startsWith('/share-login') ? null : children}
          <DebugPanel />
        </main>
      </div>
      </div>
    </div>
  )
}
