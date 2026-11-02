'use client'

import * as React from 'react'
import Link from 'next/link'
import { usePathname, useRouter, useSearchParams } from 'next/navigation'
import { Dialog as DialogPrimitive } from '@base-ui/react/dialog'
import {
  LayoutDashboard, Network, Users, Settings, Database, Search, Activity, Sparkles,
  Timer, Shield, Bell, Server, Loader2, Radio, Play, TrendingUp, X, Eye, Moon, Sun, Monitor,
} from 'lucide-react'
import { useTheme } from 'next-themes'
import { useQuery, useQueryClient } from '@tanstack/react-query'

import { cn, buildServiceHref, getTimezoneAbbr } from '@/lib/utils'
import { client, unwrapClient, queryKeys } from '@/lib/api'
import { buttonVariants } from '@/components/ui/button'
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu'
import { Select, SelectContent, SelectItem, SelectSeparator, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { ScrollArea } from '@/components/ui/scroll-area'
import { useTimezoneStore, useSessionRoleStore, useServiceStore } from '@/stores/filterStore'
import { useMounted, useUrlServiceSync, useIsAnalyst } from '@/hooks/useBootstrap'
import type { BootstrapResponse, ShareBanner } from '@/types/api'

export function SrLiveRegion({ children }: { children: React.ReactNode }) {
  return <div role="status" aria-live="polite" aria-atomic="true" className="sr-only">{children}</div>
}

export function CenteredSpinner({ label }: { label: string }) {
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
  if (!mounted) return <div className="w-[180px] h-8 border rounded-md" />
  const selectValue = mode === 'system' ? SYSTEM_TIME_VALUE : timezone

  return (
    <Select
      value={selectValue}
      onValueChange={(value) => {
        if (!value) return
        if (value === SYSTEM_TIME_VALUE) {
          try { setSystemTimezone(Intl.DateTimeFormat().resolvedOptions().timeZone) }
          catch { /* leave current zone in place */ }
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
  const hidden = React.useSyncExternalStore(subscribeVisibility, () => document.hidden, () => false)

  const readBootstrapBanner = () =>
    toBanner(queryClient.getQueryData<BootstrapResponse>(queryKeys.bootstrap())?.share_banner)

  const seeded = readBootstrapBanner()
  const seededAtRenderRef = React.useRef(seeded !== null)

  const { data: status } = useQuery<ShareBanner | null>({
    queryKey: queryKeys.shareBanner(),
    queryFn: async ({ signal }) => {
      if (queryClient.getQueryData<BootstrapResponse>(queryKeys.bootstrap())?.settings?.is_remote_analyst) return null
      if (!seededAtRenderRef.current) {
        seededAtRenderRef.current = true
        const late = readBootstrapBanner()
        if (late) return late
      }
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

  const openDashboard = () => router.push(buildServiceHref('/admin/share', activeServiceId))

  const node = enabled && status?.sharing_active ? (
    <button
      type="button"
      onClick={openDashboard}
      className="w-full bg-amber-700 hover:bg-amber-800 text-white text-xs font-semibold py-1.5 text-center shadow shrink-0"
      data-testid="share-active-banner"
    >
      ⚠️ Dashboard sharing is ACTIVE
      {status.public_url ? ` — ${status.public_url}` : ''} (click to manage)
    </button>
  ) : null

  return { node, sharingActive: !!status?.sharing_active, openDashboard }
}

export interface ServiceNavItem {
  name: string; href: string; icon: React.ComponentType<{ className?: string }>
  analystVisible: boolean; shareAnalystVisible?: boolean; requiresCmcd?: boolean; requiresRum?: boolean
}

export const SERVICE_NAVIGATION: ServiceNavItem[] = [
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

export const SYSTEM_NAVIGATION = [{ name: 'Admin', href: '/admin', icon: Settings }]

export function UrlServiceSync() {
  useUrlServiceSync()
  return null
}

const ROUTE_FRIENDLY_NAMES: Record<string, string> = {
  '/dashboard': 'Dashboard', '/control-room': 'Control Room', '/fastly-value': 'Service Summary',
  '/security': 'Security', '/network': 'Network', '/streaming': 'Streaming', '/origin': 'Origin',
  '/performance': 'Performance', '/sessions': 'Sessions', '/insights': 'Insights', '/query': 'Query',
  '/charts': 'Charts', '/alerts': 'Alerts', '/logs': 'Data Management', '/usage': 'Usage and Cost',
  '/admin': 'Admin', '/share-login': 'Sign in',
}

function friendlyPageName(pathname: string): string {
  let best = ''
  for (const key of Object.keys(ROUTE_FRIENDLY_NAMES)) {
    if ((pathname === key || pathname.startsWith(key + '/')) && key.length > best.length) best = key
  }
  return best ? ROUTE_FRIENDLY_NAMES[best] : 'Page'
}

export function RouteFocus() {
  const pathname = usePathname()
  const isFirstRender = React.useRef(true)
  const [announcement, setAnnouncement] = React.useState('')

  React.useEffect(() => {
    if (isFirstRender.current) {
      isFirstRender.current = false
      return
    }
    const main = typeof document !== 'undefined' ? document.getElementById('main') : null
    if (main) main.focus({ preventScroll: true })
    setAnnouncement(`Loaded: ${friendlyPageName(pathname)}`)
  }, [pathname])

  return <SrLiveRegion>{announcement}</SrLiveRegion>
}

export function RawQueryModeProbe({ onChange }: { onChange: (isRaw: boolean) => void }) {
  const searchParams = useSearchParams()
  const isRaw = searchParams.get('mode') === 'raw'
  React.useEffect(() => { onChange(isRaw) }, [isRaw, onChange])
  return null
}

interface NavLinkProps {
  href: string; icon: React.ElementType; name: string; isActive: boolean
  disabled?: boolean; collapsed?: boolean; onClick?: () => void; className?: string
  activeServiceId?: string | null; router: ReturnType<typeof useRouter>
}

function NavLink({ href, icon: Icon, name, isActive, disabled, collapsed, onClick, className, activeServiceId, router }: NavLinkProps) {
  const finalHref = activeServiceId ? `${href}?service=${activeServiceId}` : href
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

export function SidebarNavList({
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

export function MobileNavDrawer({
  open,
  onOpenChange,
  closeMobileNav,
  visibleNav,
  visibleSystemNav,
  pathname,
  hasServices,
  navActiveServiceId,
  router,
  isAnalyst,
  analystEmail,
  analystName,
  logout,
  isLoggingOut,
  signoutIcon,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  closeMobileNav: () => void
  visibleNav: { name: string; href: string; icon: React.ElementType }[]
  visibleSystemNav: { name: string; href: string; icon: React.ElementType }[]
  pathname: string
  hasServices: boolean
  navActiveServiceId: string | null
  router: ReturnType<typeof useRouter>
  isAnalyst: boolean
  analystEmail?: string
  analystName?: string
  logout: () => Promise<void> | void
  isLoggingOut: boolean
  signoutIcon: React.ReactNode
}) {
  return (
    <DialogPrimitive.Root open={open} onOpenChange={onOpenChange}>
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
                {signoutIcon}
                <span>Sign out</span>
              </button>
            </div>
          )}
        </DialogPrimitive.Popup>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  )
}
