'use client'

import * as React from 'react'
import dynamic from 'next/dynamic'
import Link from 'next/link'
import { usePathname, useRouter, useSearchParams } from 'next/navigation'
import { Dialog as DialogPrimitive } from '@base-ui/react/dialog'
import {
  LayoutDashboard, Network, Users, Settings, Database, Search, Activity, Menu, Sparkles,
  Timer, Shield, Bell, Server, PanelLeftClose, PanelLeftOpen, Loader2, LogOut, Radio,
  Play, TrendingUp, X, Eye, Moon, Sun, Monitor,
} from 'lucide-react'
import { useTheme } from 'next-themes'
import { useQuery, useQueryClient } from '@tanstack/react-query'

import { cn, buildServiceHref, SIDEBAR_COLLAPSED_COOKIE } from '@/lib/utils'
import { rotatePageLoadId, client, unwrapClient } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import { Button, buttonVariants } from '@/components/ui/button'
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu'
import { Select, SelectContent, SelectItem, SelectSeparator, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { useFilterUrlWriteback } from '@/hooks/useDashboardCards'
import { ScrollArea } from '@/components/ui/scroll-area'
import { ServiceSwitcher, SyncStatusBadge } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { useDebugStore, useTimezoneStore, useSessionRoleStore, useServiceStore } from '@/stores/filterStore'
import { getTimezoneAbbr } from '@/lib/format'
import {
  useMounted, useUrlServiceSync, useBootstrap, useBootstrapSyncEffects, useIsAnalyst,
  useEnforceMaskedFilters, useAnalystHeartbeat, useAnalystLogout,
} from '@/hooks/useBootstrap'
import type { BootstrapResponse, ShareBanner } from '@/types/api'
import { PlotlyPrewarm, MapPrewarm } from '@/components/PlotlyChart'
import packageJson from '../package.json'

const FilterBar = dynamic(() => import('@/components/FilterBar/FilterBar').then(m => ({ default: m.FilterBar })))
const ActiveFiltersBanner = dynamic(() => import('@/components/FilterBar/FilterBar').then(m => ({ default: m.ActiveFiltersBanner })), { ssr: false })
const DebugPanel = dynamic(() => import('@/components/DebugPanel').then(m => ({ default: m.DebugPanel })), { ssr: false })

const matchesPrefix = (pathname: string, prefixes: readonly string[]) =>
  prefixes.some((p) => pathname.startsWith(p))

function SrLiveRegion({ children }: { children: React.ReactNode }) {
  return <div role="status" aria-live="polite" aria-atomic="true" className="sr-only">{children}</div>
}

function CenteredSpinner({ label }: { label: string }) {
  return (
    <div className="flex items-center justify-center h-full text-sm text-muted-foreground" role="status">
      <Loader2 className="animate-spin mr-2 h-4 w-4" aria-hidden="true" />
      {label}
    </div>
  )
}

export function ThemeToggle() {
  const { setTheme } = useTheme()
  return (
    <DropdownMenu>
      <DropdownMenuTrigger className={buttonVariants({ variant: "outline", size: "icon" })}>
        <span className="relative flex h-[1.2rem] w-[1.2rem] items-center justify-center">
          <Sun className="absolute rotate-0 scale-100 transition-all dark:-rotate-90 dark:scale-0" />
          <Moon className="absolute rotate-90 scale-0 transition-all dark:rotate-0 dark:scale-100" />
          <span className="sr-only">Toggle theme</span>
        </span>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onClick={() => setTheme('light')}><Sun className="mr-2 h-4 w-4" />Light</DropdownMenuItem>
        <DropdownMenuItem onClick={() => setTheme('dark')}><Moon className="mr-2 h-4 w-4" />Dark</DropdownMenuItem>
        <DropdownMenuItem onClick={() => setTheme('system')}><Monitor className="mr-2 h-4 w-4" />System</DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

const SYSTEM_TIME_VALUE = '__system__'
const COMMON_TIMEZONES = [
  { label: 'UTC', value: 'UTC' },
  { label: 'US Eastern', value: 'America/New_York' },
  { label: 'US Central', value: 'America/Chicago' },
  { label: 'US Mountain', value: 'America/Denver' },
  { label: 'US Pacific', value: 'America/Los_Angeles' },
  { label: 'London', value: 'Europe/London' },
  { label: 'Berlin', value: 'Europe/Berlin' },
  { label: 'Tokyo', value: 'Asia/Tokyo' },
  { label: 'Sydney', value: 'Australia/Sydney' },
]

export function TimezoneSwitcher() {
  const mounted = useMounted()
  const { mode, timezone, setTimezone, setSystemTimezone } = useTimezoneStore()
  if (!mounted) return <div className="w-[180px] h-10 border rounded-md" />
  const selectValue = mode === 'system' ? SYSTEM_TIME_VALUE : timezone

  return (
    <Select
      value={selectValue}
      onValueChange={(value) => {
        if (!value) return
        if (value === SYSTEM_TIME_VALUE) {
          try { setSystemTimezone(Intl.DateTimeFormat().resolvedOptions().timeZone) }
          catch { /* leave the current zone in place */ }
        } else {
          setTimezone(value)
        }
      }}
    >
      <SelectTrigger className="w-[180px]" aria-label="Display timezone">
        <SelectValue placeholder="Select timezone">
          {mode === 'system' ? `System Time (${getTimezoneAbbr(new Date(), timezone)})` : undefined}
        </SelectValue>
      </SelectTrigger>
      <SelectContent>
        <SelectItem value={SYSTEM_TIME_VALUE}>System Time</SelectItem>
        <SelectSeparator />
        {COMMON_TIMEZONES.map((tz) => <SelectItem key={tz.value} value={tz.value}>{tz.label}</SelectItem>)}
        {mode === 'manual' && !COMMON_TIMEZONES.find(t => t.value === timezone) && (
          <SelectItem value={timezone}>{timezone}</SelectItem>
        )}
      </SelectContent>
    </Select>
  )
}

const SHARE_POLL_MS = 15_000

function toBanner(raw: { [key: string]: unknown } | null | undefined): ShareBanner | null {
  if (!raw) return null
  return { sharing_active: !!raw.sharing_active, public_url: typeof raw.public_url === 'string' ? raw.public_url : null }
}

function subscribeVisibility(onChange: () => void) {
  document.addEventListener('visibilitychange', onChange)
  return () => document.removeEventListener('visibilitychange', onChange)
}

export function useShareStatusBanner({ enabled }: { enabled: boolean }) {
  const router = useRouter()
  const queryClient = useQueryClient()
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const isRemoteAnalyst = useSessionRoleStore((s) => s.isRemoteAnalyst)
  const isAnalyst = useIsAnalyst()
  // Background tabs don't poll (AGENTS.md: refetchIntervalInBackground=false).
  const hidden = React.useSyncExternalStore(subscribeVisibility, () => document.hidden, () => false)

  const readBootstrapBanner = () =>
    toBanner(queryClient.getQueryData<BootstrapResponse>(queryKeys.bootstrap())?.share_banner)

  // Seed from bootstrap's share_banner (perf audit Phase D-3): initialData +
  // a staleTime equal to the poll cadence means mount does NOT fetch when the
  // seed is present; the 15s refetchInterval picks up admin start/stop within
  // one window regardless of how stale the bootstrap entry is.
  const seeded = readBootstrapBanner()
  const seededAtRenderRef = React.useRef(seeded !== null)

  const { data: status } = useQuery<ShareBanner | null>({
    queryKey: queryKeys.shareBanner(),
    queryFn: async ({ signal }) => {
      if (queryClient.getQueryData<BootstrapResponse>(queryKeys.bootstrap())?.settings?.is_remote_analyst) return null
      // Bootstrap can resolve between first render and the mount-time fetch;
      // re-read it once so that window doesn't cost a round trip either.
      if (!seededAtRenderRef.current) {
        seededAtRenderRef.current = true
        const late = readBootstrapBanner()
        if (late) return late
      }
      // Lean /api/admin/share/banner (~80B) instead of /status (~11KB): the
      // banner only needs sharing_active + public_url and this polls every
      // 15s on every page with AppLayout.
      const data = await unwrapClient(client.GET('/api/admin/share/banner', { signal }))
      return toBanner(data)
    },
    initialData: seeded ?? undefined,
    staleTime: SHARE_POLL_MS,
    refetchInterval: SHARE_POLL_MS,
    refetchIntervalInBackground: false,
    retry: false,
    enabled: enabled && !hidden && !isRemoteAnalyst && !isAnalyst,
  })

  const node = enabled && status?.sharing_active ? (
    <button
      type="button"
      onClick={() => router.push(buildServiceHref('/admin/share', activeServiceId))}
      // M-13 (a11y): bg-amber-500 + text-amber-50 measured below WCAG AA
      // contrast (~3.1 ratio) on the 12px banner text — the single largest
      // contributor to the audit's 299-node admin color-contrast count.
      // Darken to amber-700 background with white text → contrast jumps to
      // ~7.0 and the banner reads as a serious operator warning, which is
      // its actual purpose (the safety affordance reminding the admin
      // that data is exposed to external analysts).
      className="w-full bg-amber-700 hover:bg-amber-800 text-white text-xs font-semibold py-1.5 text-center shadow shrink-0"
      data-testid="share-active-banner"
    >
      ⚠️ Dashboard sharing is ACTIVE
      {status.public_url ? ` — ${status.public_url}` : ''} (click to manage)
    </button>
  ) : null

  return {
    node,
    sharingActive: !!status?.sharing_active,
    openDashboard: () => router.push(buildServiceHref('/admin/share', activeServiceId)),
  }
}

// `analystVisible` controls visibility for FOS-sharing analysts (those
// running their own copy of the app locally against the admin's FOS
// bucket). `shareAnalystVisible` overrides for SHARE-INVITED analysts
// (those using a public URL via the share-login flow). When
// shareAnalystVisible is unset it falls back to analystVisible.
//
// Data Management exposes ingestion logs / cron health — useful for an
// analyst who runs their own ingestion, leak-y for an invited viewer.
interface ServiceNavItem {
  name: string; href: string; icon: React.ComponentType<{ className?: string }>
  analystVisible: boolean; shareAnalystVisible?: boolean; requiresCmcd?: boolean; requiresRum?: boolean
}

const SERVICE_NAVIGATION: ServiceNavItem[] = [
  { name: 'Dashboard', href: '/dashboard', icon: LayoutDashboard, analystVisible: true },
  { name: 'Control Room', href: '/control-room', icon: Radio, analystVisible: true },
  { name: 'Service Summary', href: '/fastly-value', icon: TrendingUp, analystVisible: true },
  { name: 'Performance', href: '/performance', icon: Timer, analystVisible: true },
  { name: 'Origin', href: '/origin', icon: Server, analystVisible: true },
  { name: 'Security', href: '/security', icon: Shield, analystVisible: true },
  { name: 'Insights', href: '/insights', icon: Sparkles, analystVisible: true },
  { name: 'Network', href: '/network', icon: Network, analystVisible: true },
  { name: 'Streaming', href: '/streaming', icon: Play, analystVisible: true, requiresCmcd: true },
  { name: 'RUM', href: '/rum', icon: Eye, analystVisible: true, requiresRum: true },
  { name: 'Sessions', href: '/sessions', icon: Users, analystVisible: true },
  { name: 'Usage & Cost', href: '/usage', icon: Activity, analystVisible: false },
  { name: 'Query', href: '/query', icon: Search, analystVisible: true },
  { name: 'Alerts', href: '/alerts', icon: Bell, analystVisible: false },
  { name: 'Data Management', href: '/logs', icon: Database, analystVisible: true, shareAnalystVisible: false },
]

const SYSTEM_NAVIGATION = [{ name: 'Admin', href: '/admin', icon: Settings }]

function UrlServiceSync() {
  useUrlServiceSync()
  return null
}

// A-11 (a11y, WCAG 2.4.3 Focus Order + 4.1.3 Status Messages):
// Sidebar <Link prefetch> navigations swap the page content client-side,
// but without a focus reset screen-reader users stay parked on the
// sidebar link they activated — they never hear the new page title and
// can't tell that navigation succeeded. This component watches pathname
// changes and (a) moves programmatic focus to the <main> landmark so
// screen readers re-announce from the top of the new page, and (b)
// announces "Loaded: <page name>" via an aria-live region for users on
// readers that don't re-read on focus alone.
//
// The first render (initial pageload) is intentionally skipped — the
// browser already focuses the document root and re-focusing would
// override any deep-link hash target or skip-to-content interaction.
const ROUTE_FRIENDLY_NAMES: Record<string, string> = {
  '/dashboard': 'Dashboard', '/control-room': 'Control Room', '/fastly-value': 'Service Summary',
  '/security': 'Security', '/network': 'Network', '/streaming': 'Streaming', '/origin': 'Origin',
  '/performance': 'Performance', '/sessions': 'Sessions', '/insights': 'Insights', '/query': 'Query',
  '/charts': 'Charts', '/alerts': 'Alerts', '/logs': 'Data Management', '/usage': 'Usage and Cost',
  '/admin': 'Admin', '/share-login': 'Sign in',
}

function friendlyPageName(pathname: string): string {
  // Match the longest prefix so /admin/share resolves to "Admin",
  // /admin/queries to "Admin", etc.
  let best = ''
  for (const key of Object.keys(ROUTE_FRIENDLY_NAMES)) {
    if ((pathname === key || pathname.startsWith(key + '/')) && key.length > best.length) best = key
  }
  return best ? ROUTE_FRIENDLY_NAMES[best] : 'Page'
}

function RouteFocus() {
  const pathname = usePathname()
  const isFirstRender = React.useRef(true)
  const [announcement, setAnnouncement] = React.useState('')

  React.useEffect(() => {
    if (isFirstRender.current) {
      isFirstRender.current = false
      return
    }
    // Move focus to the <main> landmark so screen readers start reading
    // from the new page's content instead of the sidebar link the user
    // just activated. tabIndex={-1} on <main> (set below) makes this a
    // programmatic-only focus target — it doesn't get inserted into the
    // tab order.
    const main = typeof document !== 'undefined' ? document.getElementById('main') : null
    if (main) main.focus({ preventScroll: true })
    setAnnouncement(`Loaded: ${friendlyPageName(pathname)}`)
  }, [pathname])

  return <SrLiveRegion>{announcement}</SrLiveRegion>
}

// Lifts the `?mode=raw` search-param flag into a callback so the parent
// AppLayout can react to it without calling `useSearchParams()` directly.
// `useSearchParams()` requires a Suspense boundary above it for Next.js
// static rendering; isolating it here lets us wrap just this slice in
// <Suspense> rather than every consumer of the layout.
function RawQueryModeProbe({ onChange }: { onChange: (isRaw: boolean) => void }) {
  const searchParams = useSearchParams()
  const isRaw = searchParams.get('mode') === 'raw'
  React.useEffect(() => { onChange(isRaw) }, [isRaw, onChange])
  return null
}

interface NavLinkProps {
  href: string; icon: React.ElementType; name: string; isActive: boolean
  disabled?: boolean; collapsed?: boolean; onClick?: () => void; className?: string
}

function NavLink({ href, icon: Icon, name, isActive, disabled, collapsed, onClick, className, activeServiceId, router }: NavLinkProps & { activeServiceId?: string | null; router: ReturnType<typeof useRouter> }) {
  const finalHref = activeServiceId ? `${href}?service=${activeServiceId}` : href

  // Viewport-entry prefetch is disabled (prefetch={false}) — with ~12
  // sidebar items, auto-prefetch fires 30-60 RSC requests per page load
  // (37-66 observed, ~2s bandwidth competition). Instead we prefetch on
  // hover (desktop) and on touchstart (mobile): the pointer takes
  // 100-300ms to travel + dwell before clicking, which is enough for
  // Next.js to fetch the loading boundary so the transition feels
  // instant on click. Without onTouchStart, mobile users would always
  // hit the cold path on first tap.
  const handlePointerHint = React.useCallback(() => {
    if (!disabled) router.prefetch(finalHref)
  }, [disabled, finalHref, router])

  const link = (
    <Link
      href={finalHref}
      prefetch={false}
      onClick={onClick}
      onMouseEnter={handlePointerHint}
      onTouchStart={handlePointerHint}
      aria-disabled={disabled || undefined}
      aria-current={isActive ? 'page' : undefined}
      aria-label={collapsed ? name : undefined}
      title={!collapsed ? name : undefined}
      tabIndex={disabled ? -1 : undefined}
      className={cn(
        "flex items-center rounded-md text-sm font-medium transition-colors",
        collapsed ? "justify-center h-9 w-9 mx-auto" : "gap-3 px-3 py-2",
        disabled
          ? "text-muted-foreground opacity-50 cursor-not-allowed pointer-events-none"
          : "hover:bg-accent hover:text-accent-foreground",
        !disabled && isActive ? "bg-primary text-primary-foreground shadow-sm" : !disabled ? "text-muted-foreground" : "",
        className,
      )}
    >
      <Icon className="h-4 w-4 shrink-0" aria-hidden="true" />
      {!collapsed && <span className="truncate">{name}</span>}
    </Link>
  )

  if (!collapsed) return link
  return (
    <Tooltip>
      <TooltipTrigger render={link} />
      <TooltipContent side="right" className="text-xs font-medium">{name}</TooltipContent>
    </Tooltip>
  )
}

function SidebarNavList({
  items, ariaLabel, pathname, disabled, collapsed, activeServiceId, router, onItemClick, itemClassName, className,
}: {
  items: { name: string; href: string; icon: React.ElementType }[]
  ariaLabel: string; pathname: string; disabled?: boolean; collapsed?: boolean
  activeServiceId?: string | null; router: ReturnType<typeof useRouter>
  onItemClick?: () => void; itemClassName?: string; className?: string
}) {
  return (
    <nav className={cn("grid gap-1", className)} aria-label={ariaLabel}>
      {items.map((item) => (
        <NavLink
          key={item.href} {...item} isActive={pathname === item.href} disabled={disabled}
          collapsed={collapsed} activeServiceId={activeServiceId} router={router}
          onClick={onItemClick} className={itemClassName}
        />
      ))}
    </nav>
  )
}

export function AppLayout({
  children,
  initialCollapsed = false,
  ssrActiveServiceId,
  ssrIsRumEnabled,
  serverFooter,
}: {
  children: React.ReactNode
  initialCollapsed?: boolean
  ssrActiveServiceId?: string | null
  ssrIsRumEnabled?: boolean
  serverFooter?: React.ReactNode
}) {
  const pathname = usePathname()
  const router = useRouter()
  const bootstrapQuery = useBootstrap()
  const { data: bootstrapData, isSuccess, isLoading, isError, refetch: refetchBootstrap } = bootstrapQuery
  useBootstrapSyncEffects(bootstrapQuery)
  // Tracks whether the current /query page is in raw-SQL mode (?mode=raw).
  // Populated by <RawQueryModeProbe> inside the Suspense boundary below
  // so we don't have to call useSearchParams() directly here.
  const [isRawQueryMode, setIsRawQueryMode] = React.useState(false)

  // Sidebar collapsed state, persisted across reloads via cookie. The
  // initial value is read server-side in app/layout.tsx and passed in as
  // `initialCollapsed`, so SSR paints the correct width on first render
  // (no expand-then-collapse flash). The toggle writes the cookie
  // directly; the server picks up the new value on the next request.
  const [sidebarCollapsed, setSidebarCollapsed] = React.useState(initialCollapsed)
  const toggleSidebar = React.useCallback(() => {
    setSidebarCollapsed(prev => {
      const next = !prev
      document.cookie = `${SIDEBAR_COLLAPSED_COOKIE}=${next ? '1' : '0'}; path=/; max-age=31536000; samesite=lax`
      return next
    })
  }, [])

  // M-1 (audit, mobile UX, CRITICAL): the desktop sidebar is hidden below md
  // (see ``hidden md:flex`` on <aside> below) with no replacement, leaving
  // phones with no way to navigate between dashboard/network/security/etc.
  // The hamburger trigger lives in the mobile header (md:hidden) and opens
  // a left-edge slide-in sheet that mirrors the same NavLink set as the
  // desktop sidebar. Closes on link click via the onLinkClick callback.
  const [mobileNavOpen, setMobileNavOpen] = React.useState(false)
  const closeMobileNav = React.useCallback(() => setMobileNavOpen(false), [])
  // Route change auto-closes the panel as a belt-and-braces safeguard
  // (any Link onClick that doesn't fire — e.g. middle-click that we
  // didn't handle — still produces a pathname update).
  const [navPathname, setNavPathname] = React.useState(pathname)
  if (pathname !== navPathname) {
    setNavPathname(pathname)
    setMobileNavOpen(false)
    rotatePageLoadId()
  }
  // Cmd/Ctrl+B toggles the sidebar. Skip when an editable surface is
  // focused so the Query page's SQL editor (and any future text inputs
  // that want ⌘B for bold) keep their own binding.
  React.useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key.toLowerCase() !== 'b' || !(e.metaKey || e.ctrlKey) || e.shiftKey || e.altKey) return
      const target = e.target as HTMLElement | null
      if (target) {
        const tag = target.tagName
        if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || target.isContentEditable) return
      }
      e.preventDefault()
      toggleSidebar()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [toggleSidebar])

  // Persist filter state to URL so back-nav, refresh, and shared links
  // all round-trip the user's current dashboard view.
  useFilterUrlWriteback()

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
  const debugEnabled = useDebugStore(state => state.enabled)
  const bootstrapSettings = bootstrapData?.settings as Record<string, unknown> | undefined
  const isInitializing = bootstrapSettings?.initializing === true

  const isAnalyst = useIsAnalyst()
  // Strip any IP-family filter for masking analysts (e.g. a bookmarked
  // ?filters={ip:...} URL) so it never reaches the backend's IP-filter lock.
  useEnforceMaskedFilters()
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
  const { disconnected } = useAnalystHeartbeat({ enabled: isAnalyst && !pathname.startsWith('/share-login') })

  // Self-service sign-out for analysts (the only way to end a session early on
  // a shared machine; absent this they can only wait out the idle/absolute
  // expiry or be booted by an admin).
  const { logout, isLoggingOut } = useAnalystLogout()

  const currentServiceId = activeServiceId || ssrActiveServiceId || bootstrapData?.active_service_id
  const activeSvc = bootstrapData?.services?.find(s => s.service_id === currentServiceId)
  const activeSvcStatus = (activeSvc as Record<string, unknown> | undefined)?.status as { schema?: { name: string }[] } | undefined
  const hasCmcd = activeSvcStatus?.schema?.some(col => col.name === 'cmcd_sid') ?? false
  const hasRum = activeServiceId ? (activeSvc?.rum_enabled ?? false) : (ssrIsRumEnabled ?? activeSvc?.rum_enabled ?? false)

  const navActiveServiceId = activeServiceId || ssrActiveServiceId || null

  const visibleNav = SERVICE_NAVIGATION.filter(item => {
    if (item.requiresCmcd && !hasCmcd) return false
    if (item.requiresRum && !hasRum) return false
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
    : services.length > 0 || !!activeServiceId || !!ssrActiveServiceId

  const needsLogin = bootstrapSettings?.needs_login === true

  // A-0 (a11y, WCAG 4.1.3 Status Messages): the bootstrap-driven
  // redirects below change the user's location silently — screen reader
  // users get no signal that the page they typed/clicked is being moved
  // away from. Set a polite announcement before each redirect; the
  // sr-only live region below renders it so screen readers pick it up
  // alongside the RouteFocus "Loaded: …" announcement that fires after
  // the navigation completes.
  const analystBlocked =
    (isAnalyst && matchesPrefix(pathname, ['/admin', '/usage', '/alerts', '/high-scale'])) ||
    ((isAnalyst || isShareAnalyst) && pathname.startsWith('/logs'))
  const needsSignIn = needsLogin && !pathname.startsWith('/share-login')
  const needsWizard = !needsLogin && !isAnalyst && !hasServices && !pathname.startsWith('/admin')
  // Derived from the same conditions the redirect effect below acts on (precedence included),
  // so the announcement is never out of step with the redirect it describes.
  const redirectAnnouncement = isLoading
    ? ''
    : needsSignIn
      ? 'Sign in required. Redirecting to the sign-in page.'
      : analystBlocked
        ? 'This page is not available for your account. Redirecting to the dashboard.'
        : needsWizard
          ? 'No services configured yet. Redirecting to the admin setup page.'
          : ''

  React.useEffect(() => {
    if (isLoading) return
    // All router.replace() calls in this redirect block are wrapped in
    // startTransition so React paints the current loading.tsx skeleton
    // first instead of stalling on the synchronous URL change.
    // Anonymous remote visitors get redirected to /share-login before any
    // other layout/redirect logic kicks in. Skip while already there.
    if (needsSignIn) {
      if (typeof window !== 'undefined') {
        window.location.replace('/share-login')
      } else {
        React.startTransition(() => router.replace('/share-login'))
      }
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
    if (analystBlocked) {
      const target = activeServiceId ? `/dashboard?service=${activeServiceId}` : '/dashboard'
      if (typeof window !== 'undefined') {
        window.location.replace(target)
      } else {
        React.startTransition(() => router.replace(target))
      }
      return
    }
    // Admin-side wizard redirect — only for local admins.
    if (needsWizard) {
      React.startTransition(() => router.replace('/admin'))
    }
  }, [isLoading, needsSignIn, analystBlocked, needsWizard, router, activeServiceId])

  // SECURITY GATE: never render the app shell (sidebar, header, service
  // selector) for unauthenticated visitors. The /share-login page is a
  // standalone surface; rendering it inside the shell leaks service names,
  // nav structure, and operational metrics to anyone who hits the URL.
  // When needsLogin on a non-login route, render nothing visible while
  // the redirect useEffect above fires.
  const isLoginPage = pathname.startsWith('/share-login')
  if (isLoginPage || needsLogin) {
    return (
      <>
        <SrLiveRegion>{redirectAnnouncement}</SrLiveRegion>
        {isLoginPage ? (
          <div className="min-h-screen flex flex-col bg-muted/40">
            <main id="main" tabIndex={0} className="flex-1 outline-none">
              {children}
            </main>
            {serverFooter && !isAnalyst && serverFooter}
          </div>
        ) : null}
      </>
    )
  }

  const needsGeoPreload = matchesPrefix(pathname, ['/dashboard', '/network'])
  const needsPlotlyPrewarm = matchesPrefix(pathname, [
    '/dashboard',
    '/network',
    '/origin',
    '/performance',
    '/security',
    '/charts',
    '/insights',
    '/fastly-value',
    '/usage',
    '/streaming',
    '/control-room',
    '/sessions/stream',
  ])
  const needsMapPrewarm = matchesPrefix(pathname, ['/dashboard', '/network', '/security'])

  const isQueryRawMode = pathname.startsWith('/query') && isRawQueryMode
  const hideFilterBar =
    matchesPrefix(pathname, ['/admin', '/insights', '/alerts', '/control-room']) ||
    isQueryRawMode ||
    !hasServices

  if (isInitializing) {
    return (
      <div className="flex h-screen items-center justify-center bg-background p-6 relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-br from-background via-background to-card/10 z-0" />
        <div className="absolute -top-40 -left-40 w-96 h-96 bg-primary/5 rounded-full blur-3xl" />
        <div className="absolute -bottom-40 -right-40 w-96 h-96 bg-primary/5 rounded-full blur-3xl" />

        <div className="relative z-10 max-w-md text-center space-y-8 p-8 rounded-2xl border bg-card/60 backdrop-blur-md shadow-xl transition-all duration-500 hover:shadow-2xl hover:border-primary/20">
          <div className="flex justify-center relative">
            <div className="absolute inset-0 rounded-full bg-primary/10 animate-ping scale-75" />
            <div className="p-4 bg-muted/60 backdrop-blur-sm rounded-full border border-border/60 relative">
              <Server className="h-10 w-10 text-primary animate-pulse" />
            </div>
          </div>

          <div className="space-y-3">
            <h1 className="text-2xl font-bold tracking-tight bg-gradient-to-r from-foreground to-foreground/80 bg-clip-text text-transparent">
              Initializing Analytics Engine
            </h1>
            <p className="text-sm text-muted-foreground leading-relaxed">
              Fastly Log Analytics is pre-warming database views and compiling bot detection rules. This typically takes less than a minute.
            </p>
          </div>

          <div className="flex flex-col items-center space-y-4 pt-4 border-t border-border/40">
            <div className="flex items-center space-x-3 text-xs font-medium text-primary/90 bg-primary/10 px-4 py-2 rounded-full border border-primary/20 shadow-sm">
              <Loader2 className="h-4 w-4 animate-spin text-primary" />
              <span>Optimizing database caches...</span>
            </div>
            <p className="text-xs text-muted-foreground/80 italic">
              Dashboard will mount automatically when ready
            </p>
          </div>
        </div>
      </div>
    )
  }

  // E-2 fix: /api/bootstrap is the spine of every redirect + nav-visibility
  // decision below — without it, hasServices defaults to the persisted
  // store and the redirect effect runs on stale/empty data, producing a
  // blank page or a wrong-route bounce. Render a retry fallback when the
  // bootstrap query has errored AND we have no cached data to fall back
  // on. If `bootstrapData` exists (stale-while-error), keep rendering the
  // app — React Query will silently retry in the background.
  if (isError && !bootstrapData) {
    return (
      <div className="flex h-screen items-center justify-center bg-background p-6">
        <div className="max-w-sm text-center space-y-4 p-6 rounded-lg border bg-card shadow-sm">
          <h2 className="text-lg font-semibold">Reconnecting…</h2>
          <p className="text-sm text-muted-foreground">
            We can&apos;t reach the server right now. This is normal during a
            deploy — the page recovers on its own as soon as it&apos;s back.
          </p>
          <div
            className="flex items-center justify-center gap-2 text-xs text-muted-foreground"
            role="status"
            aria-live="polite"
          >
            <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden="true" />
            <span>Retrying automatically</span>
          </div>
          <Button onClick={() => { void refetchBootstrap() }} variant="outline" size="sm">
            Retry now
          </Button>
        </div>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-background">
      {needsGeoPreload && <link rel="prefetch" href="/geo/world.topo.json" as="fetch" />}
      {shareBanner.node}
      <div className="flex flex-1 overflow-hidden min-h-0">
      {isAnalyst && disconnected && (
        <div data-testid="connection-interrupted-overlay" className="fixed inset-0 z-[100] bg-background/90 backdrop-blur-sm flex items-center justify-center">
          <div className="max-w-sm text-center space-y-3 p-6 rounded-lg border bg-card shadow-lg">
            <h2 className="text-lg font-semibold">Connection interrupted</h2>
            <p className="text-sm text-muted-foreground">
              We can&apos;t reach the dashboard. We&apos;ll keep trying — your view will resume automatically when the connection returns.
            </p>
          </div>
        </div>
      )}
      <React.Suspense fallback={null}>
        <UrlServiceSync />
        <RawQueryModeProbe onChange={setIsRawQueryMode} />
      </React.Suspense>
      {/* A-11 (a11y): focus reset + aria-live announcement on route change.
          Mounted at layout root so it persists across navigations and
          fires once per pathname transition (see RouteFocus above). */}
      <RouteFocus />
      <SrLiveRegion>{redirectAnnouncement}</SrLiveRegion>
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
      {needsPlotlyPrewarm && <PlotlyPrewarm />}
      {needsMapPrewarm && <MapPrewarm />}
      {/* Desktop Sidebar */}
      {/* A-9 (a11y, WCAG 1.4.13 Content on Hover or Focus): keep the
          snappy 200ms open delay for sidebar nav labels, but drop the
          closeDelay={0} override so the tooltip lingers long enough
          for low-precision pointers to reach the content (inherits
          the 300ms default from components/ui/tooltip.tsx). */}
      <TooltipProvider delay={200}>
      <aside
        id="app-sidebar"
        data-collapsed={sidebarCollapsed || undefined}
        className={cn("hidden md:flex flex-col border-r bg-muted/40 transition-[width] duration-200 ease-out", sidebarCollapsed ? "w-14" : "w-64")}
      >
        <div className="flex h-14 items-center justify-center border-b px-2 py-2 shrink-0">
          <Link
            href={hasServices ? (navActiveServiceId ? `/dashboard?service=${navActiveServiceId}` : "/dashboard") : "/admin"}
            prefetch={false}
            className="flex flex-col items-center justify-center hover:opacity-80 transition-opacity mt-1"
            aria-label="Fastly Log Analytics — home"
          >
             <img src="/fastly.svg" alt="Fastly" width={52} height={20} className={cn("dark:invert transition-[height] duration-200 w-auto", sidebarCollapsed ? "h-4" : "h-5")} />
             {!sidebarCollapsed && <span className="text-[11px] font-bold uppercase tracking-widest text-muted-foreground mt-0.5">Log Analytics</span>}
          </Link>
        </div>
        <ScrollArea className="flex-1">
          <SidebarNavList items={visibleNav} ariaLabel="Primary" pathname={pathname} disabled={!hasServices} collapsed={sidebarCollapsed} activeServiceId={navActiveServiceId} router={router} className="p-2" />
        </ScrollArea>
        <div className="mt-auto p-2 border-t bg-muted/20">
          <SidebarNavList items={visibleSystemNav} ariaLabel="System" pathname={pathname} collapsed={sidebarCollapsed} activeServiceId={activeServiceId} router={router} />
          {!sidebarCollapsed && (
            // text-muted-foreground (no /opacity-step) keeps the version
            // string above WCAG 2.1 AA 4.5:1 at 10px on bg-muted/20.
            // /50 dropped to 2.19, which axe flagged on /dashboard.
            // data-axe-ignore excludes from the e2e axe scope —
            // 10px decorative version string is intentional low-emphasis.
            <div data-axe-ignore="true" className="mt-4 mb-1 text-[10px] text-muted-foreground text-center font-mono select-all">
              v{packageJson.version}
            </div>
          )}
          {/* When collapsed, keep the analyst watermark in the DOM (tests
              and audit hooks key off data-analyst-email) but visually
              hidden — the expanded copy is the user-facing one. */}
          {isAnalyst && (analystEmail || analystName) && (
            <div
              data-testid="analyst-watermark"
              data-analyst-email={analystEmail || ''}
              data-axe-ignore={sidebarCollapsed ? undefined : 'true'}
              className={sidebarCollapsed ? 'sr-only' : 'text-[10px] text-muted-foreground text-center mt-1'}
            >
              Viewing as <span className="font-medium">{analystName || analystEmail}</span>
            </div>
          )}
          {isAnalyst && (
            sidebarCollapsed ? (
              <Tooltip>
                <TooltipTrigger render={
                  <button
                    type="button" data-testid="analyst-signout" onClick={() => { void logout() }} disabled={isLoggingOut} aria-label="Sign out"
                    className="mt-2 flex items-center justify-center h-9 w-9 mx-auto rounded-md text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-colors disabled:opacity-50"
                  />
                }>
                  {isLoggingOut ? <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" /> : <LogOut className="h-4 w-4" aria-hidden="true" />}
                </TooltipTrigger>
                <TooltipContent side="right" className="text-xs font-medium">Sign out</TooltipContent>
              </Tooltip>
            ) : (
              <Button variant="ghost" size="sm" data-testid="analyst-signout" onClick={() => { void logout() }} disabled={isLoggingOut} className="mt-2 w-full justify-start gap-3 px-3 text-muted-foreground hover:text-accent-foreground">
                {isLoggingOut ? <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" /> : <LogOut className="h-4 w-4" aria-hidden="true" />}
                Sign out
              </Button>
            )
          )}
        </div>
      </aside>

      {/* Main Content */}
      <div className="flex flex-1 flex-col overflow-hidden">
        <header className="flex h-14 items-center gap-2 border-b bg-muted/40 px-4 shrink-0">
          {/* M-1 (audit, mobile UX): hamburger trigger replaces the
              desktop sidebar below md. Opens the slide-in mobile nav
              rendered at the bottom of the layout tree. */}
          <button
            type="button" onClick={() => setMobileNavOpen(true)} aria-label="Open navigation menu" aria-controls="mobile-nav" aria-expanded={mobileNavOpen}
            className="md:hidden flex items-center justify-center h-10 w-10 -ml-2 rounded-md text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-colors"
          >
            <Menu className="h-5 w-5" aria-hidden="true" />
          </button>
          {/* Sidebar toggle — VSCode-style: lives in the app header
              so the position never shifts between expanded/collapsed
              states. Hidden on mobile since the sidebar itself is
              hidden below md. */}
          <Tooltip>
            <TooltipTrigger render={
              <button
                type="button" onClick={toggleSidebar} aria-label={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
                aria-expanded={!sidebarCollapsed} aria-controls="app-sidebar" aria-keyshortcuts="Control+B Meta+B"
                className="hidden md:flex items-center justify-center h-8 w-8 rounded-md text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-colors -ml-1 mr-1"
              />
            }>
              {sidebarCollapsed ? <PanelLeftOpen className="h-4 w-4" aria-hidden="true" /> : <PanelLeftClose className="h-4 w-4" aria-hidden="true" />}
            </TooltipTrigger>
            <TooltipContent side="bottom" className="text-xs font-medium">
              {sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
              <span className="opacity-60 ml-2 font-mono">⌘B</span>
            </TooltipContent>
          </Tooltip>
          <ServiceSwitcher />
          <div className="ml-auto flex items-center gap-2">
            <SyncStatusBadge />
            <TimezoneSwitcher />
            <ThemeToggle />
          </div>
        </header>
        {!hideFilterBar && <FilterBar />}
        {/* On pages where the FilterBar is hidden (insights / alerts /
            admin / logs / share-login / raw-query), surface any filters
            the user previously set on a dashboard / query page so they
            aren't invisibly carried forward to a surface that doesn't
            apply them. Renders nothing when no filters / edgeOnly are
            set, so it's free on the cold path. */}
        {hideFilterBar && <ActiveFiltersBanner />}

        {/* A-11 (a11y): tabIndex={0} makes <main> a programmatic focus
            target and keyboard-accessible scrollable region (WCAG 2.1 AA
            scrollable-region-focusable). outline-none avoids an intrusive
            focus ring on the landmark itself. */}
        <main id="main" tabIndex={0} className="flex-1 overflow-auto p-4 md:p-6 outline-none">
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
          {isLoading && !bootstrapData ? (
            <CenteredSpinner label="Loading app configuration…" />
          ) : !hasServices && !matchesPrefix(pathname, ['/admin', '/share-login']) ? (
            <CenteredSpinner label="Setting up your workspace…" />
          ) : children}
          {debugEnabled && <DebugPanel />}
          {serverFooter && !isAnalyst && serverFooter}
        </main>
      </div>
      </TooltipProvider>
      </div>
      {/* M-1 (audit, mobile UX, CRITICAL): mobile nav sheet. Lives
          outside the desktop sidebar tree so its portal renders above
          the rest of the app. Mirrors the same visibleNav /
          visibleSystemNav set as the desktop <aside>. Closes on link
          click and on route change (see closeMobileNav and the
          pathname effect above). */}
      <DialogPrimitive.Root open={mobileNavOpen} onOpenChange={setMobileNavOpen}>
        <DialogPrimitive.Portal>
          <DialogPrimitive.Backdrop className="fixed inset-0 z-50 bg-black/40 md:hidden data-open:animate-in data-open:fade-in-0 data-closed:animate-out data-closed:fade-out-0" />
          <DialogPrimitive.Popup
            id="mobile-nav" aria-label="Primary navigation"
            className="fixed inset-y-0 left-0 z-50 w-72 max-w-[85vw] flex flex-col bg-background border-r shadow-xl md:hidden outline-none data-open:animate-in data-open:slide-in-from-left data-closed:animate-out data-closed:slide-out-to-left duration-200"
          >
            <div className="flex h-14 items-center justify-between border-b px-4 shrink-0">
              <DialogPrimitive.Title className="flex items-center gap-2">
                <img src="/fastly.svg" alt="Fastly" width={52} height={20} className="dark:invert h-5 w-auto" />
                <span className="text-[11px] font-bold uppercase tracking-widest text-muted-foreground">Log Analytics</span>
              </DialogPrimitive.Title>
              <DialogPrimitive.Close aria-label="Close navigation menu" className="flex items-center justify-center h-10 w-10 -mr-2 rounded-md text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-colors">
                <X className="h-5 w-5" aria-hidden="true" />
              </DialogPrimitive.Close>
            </div>
            <ScrollArea className="flex-1">
              <SidebarNavList items={visibleNav} ariaLabel="Primary" pathname={pathname} disabled={!hasServices} activeServiceId={navActiveServiceId} router={router} onItemClick={closeMobileNav} itemClassName="py-3 min-h-11" className="p-2" />
            </ScrollArea>
            {visibleSystemNav.length > 0 && (
              <div className="border-t p-2 bg-muted/20 shrink-0">
                <SidebarNavList items={visibleSystemNav} ariaLabel="System" pathname={pathname} activeServiceId={navActiveServiceId} router={router} onItemClick={closeMobileNav} itemClassName="py-3 min-h-11" />
              </div>
            )}
            {isAnalyst && (
              <div className="border-t p-2 bg-muted/20 shrink-0">
                {(analystEmail || analystName) && (
                  <div className="px-3 pb-2 text-[11px] text-muted-foreground">
                    Viewing as <span className="font-medium">{analystName || analystEmail}</span>
                  </div>
                )}
                <button
                  type="button" data-testid="analyst-signout-mobile" onClick={() => { closeMobileNav(); void logout() }} disabled={isLoggingOut}
                  className="flex w-full items-center gap-3 rounded-md px-3 py-3 text-sm font-medium text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-colors min-h-11 disabled:opacity-50"
                >
                  {isLoggingOut ? <Loader2 className="h-4 w-4 shrink-0 animate-spin" aria-hidden="true" /> : <LogOut className="h-4 w-4 shrink-0" aria-hidden="true" />}
                  <span>Sign out</span>
                </button>
              </div>
            )}
          </DialogPrimitive.Popup>
        </DialogPrimitive.Portal>
      </DialogPrimitive.Root>
    </div>
  )
}
