'use client'

import * as React from 'react'
import dynamic from 'next/dynamic'
import Link from 'next/link'
import { usePathname, useRouter } from 'next/navigation'
import { Menu, Server, PanelLeftClose, PanelLeftOpen, Loader2, LogOut } from 'lucide-react'

import { cn, SIDEBAR_COLLAPSED_COOKIE } from "@/lib/utils";
import { rotatePageLoadId } from "@/lib/api";
import { Button } from '@/components/ui/button'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { useFilterUrlWriteback } from '@/hooks/useDashboardCards'
import { ScrollArea } from '@/components/ui/scroll-area'
import { ServiceSwitcher, SyncStatusBadge } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { useDebugStore, useServiceStore } from '@/stores/filterStore'
import {
  useBootstrap, useBootstrapSyncEffects, useIsAnalyst,
  useEnforceMaskedFilters, useAnalystHeartbeat, useAnalystLogout,
} from '@/hooks/useBootstrap'
import { PlotlyPrewarm, MapPrewarm } from '@/components/PlotlyChart'
import packageJson from '../package.json'
import {
  SrLiveRegion,
  CenteredSpinner,
  ThemeToggle,
  TimezoneSwitcher,
  useShareStatusBanner,
  SERVICE_NAVIGATION,
  SYSTEM_NAVIGATION,
  UrlServiceSync,
  RouteFocus,
  RawQueryModeProbe,
  SidebarNavList,
  MobileNavDrawer,
} from './AppLayoutChrome'

export { ThemeToggle, TimezoneSwitcher, useShareStatusBanner }

const FilterBar = dynamic(() => import('@/components/FilterBar/FilterBar').then(m => ({ default: m.FilterBar })))
const ActiveFiltersBanner = dynamic(() => import('@/components/FilterBar/FilterBar').then(m => ({ default: m.ActiveFiltersBanner })), { ssr: false })
const DebugPanel = dynamic(() => import('@/components/DebugPanel').then(m => ({ default: m.DebugPanel })), { ssr: false })

const matchesPrefix = (pathname: string, prefixes: readonly string[]) =>
  prefixes.some((p) => pathname.startsWith(p))

const PLOTLY_PREWARM_PREFIXES = [
  '/dashboard', '/network', '/origin', '/performance', '/security', '/charts',
  '/insights', '/fastly-value', '/usage', '/streaming', '/control-room', '/sessions/stream',
] as const

export function AppLayout({
  children, initialCollapsed = false, ssrActiveServiceId, ssrIsRumEnabled, serverFooter,
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
  const [isRawQueryMode, setIsRawQueryMode] = React.useState(false)

  const [sidebarCollapsed, setSidebarCollapsed] = React.useState(initialCollapsed)
  const toggleSidebar = React.useCallback(() => {
    setSidebarCollapsed(prev => {
      const next = !prev
      document.cookie = `${SIDEBAR_COLLAPSED_COOKIE}=${next ? '1' : '0'}; path=/; max-age=31536000; samesite=lax`
      return next
    })
  }, [])

  const [mobileNavOpen, setMobileNavOpen] = React.useState(false)
  const closeMobileNav = React.useCallback(() => setMobileNavOpen(false), [])
  const [navPathname, setNavPathname] = React.useState(pathname)
  if (pathname !== navPathname) {
    setNavPathname(pathname)
    setMobileNavOpen(false)
    rotatePageLoadId()
  }

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

  useFilterUrlWriteback()

  const activeServiceId = useServiceStore(state => state.activeServiceId)
  const services = useServiceStore(state => state.services)
  const debugEnabled = useDebugStore(state => state.enabled)
  const bootstrapSettings = bootstrapData?.settings as Record<string, unknown> | undefined
  const isInitializing = bootstrapSettings?.initializing === true

  const isAnalyst = useIsAnalyst()
  useEnforceMaskedFilters()
  const isShareAnalyst = bootstrapSettings?.is_remote_analyst === true
  const analystEmail = (bootstrapSettings?.analyst_email as string | undefined) || undefined
  const analystName = (bootstrapSettings?.analyst_name as string | undefined) || undefined

  const shareBanner = useShareStatusBanner({ enabled: isSuccess && !isAnalyst })
  const { disconnected } = useAnalystHeartbeat({ enabled: isAnalyst && !pathname.startsWith('/share-login') })
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
    if (isShareAnalyst) return item.shareAnalystVisible ?? item.analystVisible ?? false
    if (isAnalyst) return item.analystVisible
    return true
  })
  const visibleSystemNav = isAnalyst ? [] : SYSTEM_NAVIGATION

  const hasServices = isSuccess
    ? !!(bootstrapData?.services?.length)
    : services.length > 0 || !!activeServiceId || !!ssrActiveServiceId

  const needsLogin = bootstrapSettings?.needs_login === true
  const analystBlocked =
    (isAnalyst && matchesPrefix(pathname, ['/admin', '/usage', '/alerts', '/high-scale'])) ||
    ((isAnalyst || isShareAnalyst) && pathname.startsWith('/logs'))
  const needsSignIn = needsLogin && !pathname.startsWith('/share-login')
  const needsWizard = !needsLogin && !isAnalyst && !hasServices && !pathname.startsWith('/admin')

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
    if (needsSignIn) {
      if (typeof window !== 'undefined') window.location.replace('/share-login')
      else React.startTransition(() => router.replace('/share-login'))
      return
    }
    if (analystBlocked) {
      const target = activeServiceId ? `/dashboard?service=${activeServiceId}` : '/dashboard'
      if (typeof window !== 'undefined') window.location.replace(target)
      else React.startTransition(() => router.replace(target))
      return
    }
    if (needsWizard) {
      React.startTransition(() => router.replace('/admin'))
    }
  }, [isLoading, needsSignIn, analystBlocked, needsWizard, router, activeServiceId])

  const isLoginPage = pathname.startsWith('/share-login')
  if (isLoginPage || needsLogin) {
    return (
      <>
        <SrLiveRegion>{redirectAnnouncement}</SrLiveRegion>
        {isLoginPage ? (
          <div className="min-h-screen flex flex-col bg-muted/40">
            <main id="main" tabIndex={0} className="flex-1 outline-none">{children}</main>
            {serverFooter && !isAnalyst && serverFooter}
          </div>
        ) : null}
      </>
    )
  }

  const needsGeoPreload = matchesPrefix(pathname, ['/dashboard', '/network'])
  const needsPlotlyPrewarm = matchesPrefix(pathname, PLOTLY_PREWARM_PREFIXES)
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
            <p className="text-xs text-muted-foreground/80 italic">Dashboard will mount automatically when ready</p>
          </div>
        </div>
      </div>
    )
  }

  if (isError && !bootstrapData) {
    return (
      <div className="flex h-screen items-center justify-center bg-background p-6">
        <div className="max-w-sm text-center space-y-4 p-6 rounded-lg border bg-card shadow-sm">
          <h2 className="text-lg font-semibold">Reconnecting…</h2>
          <p className="text-sm text-muted-foreground">
            We can&apos;t reach the server right now. This is normal during a deploy — the page recovers on its own as soon as it&apos;s back.
          </p>
          <div className="flex items-center justify-center gap-2 text-xs text-muted-foreground" role="status" aria-live="polite">
            <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden="true" />
            <span>Retrying automatically</span>
          </div>
          <Button onClick={() => { void refetchBootstrap() }} variant="outline" size="sm">Retry now</Button>
        </div>
      </div>
    )
  }

  const signoutIcon = isLoggingOut
    ? <Loader2 className="h-4 w-4 shrink-0 animate-spin" aria-hidden="true" />
    : <LogOut className="h-4 w-4 shrink-0" aria-hidden="true" />

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
        <RouteFocus />
        <SrLiveRegion>{redirectAnnouncement}</SrLiveRegion>
        {needsPlotlyPrewarm && <PlotlyPrewarm />}
        {needsMapPrewarm && <MapPrewarm />}
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
                <div data-axe-ignore="true" className="mt-4 mb-1 text-[10px] text-muted-foreground text-center font-mono select-all">
                  v{packageJson.version}
                </div>
              )}
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
                      {signoutIcon}
                    </TooltipTrigger>
                    <TooltipContent side="right" className="text-xs font-medium">Sign out</TooltipContent>
                  </Tooltip>
                ) : (
                  <Button variant="ghost" size="sm" data-testid="analyst-signout" onClick={() => { void logout() }} disabled={isLoggingOut} className="mt-2 w-full justify-start gap-3 px-3 text-muted-foreground hover:text-accent-foreground">
                    {signoutIcon}
                    Sign out
                  </Button>
                )
              )}
            </div>
          </aside>

          {/* Main Content */}
          <div className="flex flex-1 flex-col overflow-hidden">
            <header className="flex h-14 items-center gap-2 border-b bg-muted/40 px-4 shrink-0">
              <button
                type="button" onClick={() => setMobileNavOpen(true)} aria-label="Open navigation menu" aria-controls="mobile-nav" aria-expanded={mobileNavOpen}
                className="md:hidden flex items-center justify-center h-10 w-10 -ml-2 rounded-md text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-colors"
              >
                <Menu className="h-5 w-5" aria-hidden="true" />
              </button>
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
            {hideFilterBar && <ActiveFiltersBanner />}

            <main id="main" tabIndex={0} className="flex-1 overflow-auto p-4 md:p-6 outline-none">
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

      <MobileNavDrawer
        open={mobileNavOpen}
        onOpenChange={setMobileNavOpen}
        closeMobileNav={closeMobileNav}
        visibleNav={visibleNav}
        visibleSystemNav={visibleSystemNav}
        pathname={pathname}
        hasServices={hasServices}
        navActiveServiceId={navActiveServiceId}
        router={router}
        isAnalyst={isAnalyst}
        analystEmail={analystEmail}
        analystName={analystName}
        logout={logout}
        isLoggingOut={isLoggingOut}
        signoutIcon={signoutIcon}
      />
    </div>
  )
}
