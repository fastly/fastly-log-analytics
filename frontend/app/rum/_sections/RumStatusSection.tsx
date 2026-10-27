'use client'

import React, { useState, useEffect } from 'react'
import { useSearchParams } from 'next/navigation'
import { parseAsString, useQueryState } from 'nuqs'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Activity, AlertTriangle, RefreshCw, TriangleAlert } from 'lucide-react'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { DisableRumDialog, EnableRumDialog, RumSettingsDialog } from '@/components/Rum/RumSettingsDialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { CopyButton } from '@/components/ui/confirm-dialog'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { useIsAnalyst } from '@/hooks/useBootstrap'
import { useRumStatus } from '@/hooks/useDashboardCards'
import { adminJsonResult, client, unwrapClient, queryKeys } from '@/lib/api'
import { formatLocalDateTime, cn } from '@/lib/utils'
import { useServiceStore } from '@/stores/filterStore'
import type { WithRequired, components } from '@/types/api'

export type RumVersionsResponse = components['schemas']['RumVersionsResponse']
export type RumWire = components['schemas']['RumAnalyticsResponse']
export type RumAnalytics = WithRequired<RumWire, 'beacon_count' | 'vitals' | 'worst_pages' | 'errors' | 'trends' | 'environments'>

export interface RumFaroVersionCardProps {
  serviceId: string | null
}

export function RumFaroVersionCard({ serviceId }: RumFaroVersionCardProps) {
  const queryClient = useQueryClient()
  const [isUpgradeOpen, setIsUpgradeOpen] = useState(false)
  const isAnalyst = useIsAnalyst()
  const { data: status, isLoading: isStatusLoading } = useRumStatus(serviceId)
  const rumEnabled = !!status?.enabled

  const { data, isLoading: isVersionsLoading, isError, refetch, isFetching } = useQuery<RumVersionsResponse>({
    queryKey: queryKeys.rumVersions(serviceId),
    queryFn: ({ signal }) => unwrapClient(client.GET('/api/services/{service_id}/rum/versions', { params: { path: { service_id: serviceId as string } }, signal })),
    enabled: !!serviceId && rumEnabled && !isAnalyst,
    retry: false,
  })

  if (isAnalyst) return null
  if (!serviceId || isStatusLoading || (rumEnabled && isVersionsLoading)) {
    return <div className="flex items-center gap-2 shrink-0"><Skeleton className="h-8 w-32" /><Skeleton className="h-8 w-24" /></div>
  }
  if (!rumEnabled) return null

  if (isError) {
    return (
      <div className="flex items-center gap-2 text-xs text-destructive bg-destructive/10 px-2.5 py-1 rounded-md border border-destructive/20 shrink-0">
        <TriangleAlert className="h-3.5 w-3.5 shrink-0" />
        <span>Versions offline</span>
        <Button variant="ghost" size="icon" onClick={() => refetch()} disabled={isFetching} aria-label="Retry" className="h-5 w-5 hover:bg-destructive/20 hover:text-destructive shrink-0">
          <RefreshCw className={cn('h-2.5 w-2.5', isFetching && 'animate-spin')} />
        </Button>
      </div>
    )
  }

  const { available = [], current = null, latest = null, update_available: updateAvailable = false } = data ?? {}
  const handleComplete = () => {
    queryClient.invalidateQueries({ queryKey: queryKeys.rumVersions(serviceId) })
    queryClient.invalidateQueries({ queryKey: queryKeys.rumStatus(serviceId) })
  }

  return (
    <>
      <div className="flex items-center gap-2 shrink-0" data-testid="admin-rum-faro-version-card">
        <div className="flex items-center gap-1.5 text-xs text-muted-foreground mr-1">
          <span className="font-semibold uppercase tracking-wider text-[10px]">Faro SDK:</span>
          <span className="font-mono font-medium text-foreground bg-muted/60 px-1.5 py-0.5 rounded border border-muted-foreground/10" data-testid="admin-rum-faro-version">
            {current ?? 'Not pinned'}
          </span>
          {updateAvailable && <Badge variant="warning" className="text-[9px] py-0 px-1.5 h-4.5 font-medium ml-1 shrink-0">Update available</Badge>}
        </div>
        <Button onClick={() => setIsUpgradeOpen(true)} variant={updateAvailable ? 'default' : 'outline'} size="sm" className="h-8 text-xs font-semibold px-3" data-testid="admin-rum-settings-btn">
          RUM Settings
        </Button>
      </div>
      <RumSettingsDialog
        serviceId={serviceId} open={isUpgradeOpen} onOpenChange={setIsUpgradeOpen}
        availableVersions={available} currentVersion={current} latestVersion={latest} onComplete={handleComplete}
      />
    </>
  )
}

export function RumStatusPanel() {
  const searchParams = useSearchParams()
  const queryClient = useQueryClient()
  let nuqsServiceId: string | null = null
  try {
    ;[nuqsServiceId] = useQueryState('service', parseAsString)
  } catch {
    // Fallback when NuqsAdapter is not mounted in unit tests
  }
  const queryServiceId = nuqsServiceId ?? searchParams?.get('service')
  const storeServiceId = useServiceStore((s) => s.activeServiceId)
  const setActiveServiceId = useServiceStore((s) => s.setActiveServiceId)

  useEffect(() => {
    if (queryServiceId && queryServiceId !== storeServiceId) setActiveServiceId(queryServiceId)
  }, [queryServiceId, storeServiceId, setActiveServiceId])

  const { data: services, isLoading: isServicesLoading, refetch: refetchServices } = useQuery({
    queryKey: queryKeys.services(),
    queryFn: async () => {
      const res = await adminJsonResult('/api/services', '/api/services')
      return res.ok ? res.data : null
    },
  })

  const serviceId = queryServiceId || storeServiceId || services?.services?.[0]?.service_id || null
  const isAnalyst = useIsAnalyst()
  const [isEnableOpen, setIsEnableOpen] = useState(false)
  const [isDisableOpen, setIsDisableOpen] = useState(false)

  const { data: status, isLoading: isStatusLoading, isFetching: isStatusFetching, refetch: refetchStatus } =
    useRumStatus(serviceId, { refetchInterval: 30_000 })

  const currentService = services?.services?.find((s: components['schemas']['ServiceConfig']) => s.service_id === serviceId) || null
  const isInitialLoading = (!serviceId && isServicesLoading) || (!!serviceId && isStatusLoading && !status)
  const isRefreshing = isStatusFetching && !isInitialLoading

  const handleManualRefresh = async () => {
    await Promise.all([
      refetchStatus(),
      refetchServices(),
      serviceId ? queryClient.invalidateQueries({ queryKey: ['rum-versions', serviceId] }) : Promise.resolve(),
    ])
  }

  if (isAnalyst && (status?.unauthorized || (!isInitialLoading && !status?.enabled))) {
    const denied = !!status?.unauthorized
    return (
      <AnalyticsCard title="RUM Status">
        <div className="flex flex-col items-center justify-center gap-4 py-16 text-center">
          <AlertTriangle className={cn('h-12 w-12', denied ? 'text-destructive' : 'text-muted-foreground')} />
          <div>
            <h3 className={cn('text-lg font-medium', denied && 'text-destructive')}>{denied ? 'Access Denied' : 'RUM not enabled'}</h3>
            <p className="text-sm text-muted-foreground mt-1">
              {denied
                ? 'You are not authorized to view RUM data for this service. Please contact your administrator to grant access.'
                : 'Real User Monitoring is not enabled for this service. Ask an administrator to enable it.'}
            </p>
          </div>
        </div>
      </AnalyticsCard>
    )
  }

  const enabledCollectors = status
    ? [
        status.capture_vitals !== false && 'Vitals',
        status.capture_performance !== false && 'Performance',
        status.capture_errors !== false && 'Errors',
        status.capture_events !== false && 'Events',
      ].filter(Boolean)
    : []

  return (
    <div className="space-y-4" data-testid="admin-rum-panel">
      <div
        className="flex items-center justify-between gap-3 rounded-lg border bg-card px-4 py-3 h-[56px] min-h-[56px] [contain-intrinsic-size:auto_56px] [content-visibility:auto]"
        data-testid="admin-rum-toolbar"
      >
        <div className="flex items-center gap-3 min-w-0">
          <Badge
            variant={isInitialLoading ? 'secondary' : status?.enabled ? 'default' : 'outline'}
            className="min-w-[152px] justify-center shrink-0" data-testid="admin-rum-status-badge"
          >
            {isInitialLoading ? 'Checking...' : status?.enabled ? 'Beacon Pipeline Active' : 'Beacon Pipeline Inactive'}
          </Badge>
          <span className="text-xs text-muted-foreground truncate" data-testid="admin-rum-service-label">
            Service: <strong className="text-foreground">{currentService?.name ?? serviceId ?? 'Loading...'}</strong>
          </span>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          <RumFaroVersionCard serviceId={serviceId} />
          <Button variant="outline" size="sm" onClick={handleManualRefresh} disabled={isStatusFetching || !serviceId} data-testid="admin-rum-refresh-btn" className="h-8 text-xs shrink-0">
            <RefreshCw className={cn('mr-2 h-3.5 w-3.5', isStatusFetching && 'animate-spin')} />
            Refresh
          </Button>
        </div>
      </div>

      <div className="min-h-[260px] [contain-intrinsic-size:auto_260px] [content-visibility:auto]" data-testid="admin-rum-status-reservation">
        <AnalyticsCard title="RUM Status" className={cn('space-y-4 min-h-[260px]', isRefreshing && 'opacity-40 pointer-events-none transition-opacity')}>
          <div data-testid="admin-rum-status-card" className="space-y-4">
            {isInitialLoading ? (
              <div className="space-y-4" data-testid="admin-rum-status-skeleton">
                <div className="space-y-2 min-h-[112px]">
                  <p className="text-xs text-muted-foreground">Inspecting RUM beacon pipeline...</p>
                  <Skeleton className="h-4 w-48" /><Skeleton className="h-4 w-64" /><Skeleton className="h-4 w-40" />
                </div>
                <div className="flex gap-2"><Skeleton className="h-9 w-28" /><Skeleton className="h-9 w-28" /></div>
              </div>
            ) : (
              <>
                <div className="space-y-2 min-h-[112px]">
                  <p className="text-sm" data-testid="admin-rum-status-line">
                    <strong>Status:</strong> {status?.enabled ? '✅ Enabled' : '❌ Disabled'}
                  </p>
                  {status?.enabled && (
                    <>
                      <p className="text-sm" data-testid="admin-rum-enabled-at">
                        <strong>Enabled at:</strong> {status.enabled_at ? formatLocalDateTime(status.enabled_at) : 'Unknown'}
                      </p>
                      <p className="text-sm" data-testid="admin-rum-vcl-drift">
                        <strong>VCL Drift:</strong> {status.vcl_drift ? '⚠️ Yes' : '✅ No'}
                      </p>
                      {!isAnalyst && (
                        <p className="text-sm" data-testid="admin-rum-capture-summary">
                          <strong>Collectors:</strong> {enabledCollectors.length > 0 ? enabledCollectors.join(', ') : 'None'}
                          {status.custom_condition ? ` · VCL Condition: ${status.custom_condition}` : ''}
                        </p>
                      )}
                    </>
                  )}
                </div>

                <div className="flex gap-2">
                  <Button onClick={() => setIsEnableOpen(true)} disabled={status?.enabled} variant={status?.enabled ? 'secondary' : 'default'} data-testid="admin-rum-enable-btn">
                    Enable RUM
                  </Button>
                  <Button onClick={() => setIsDisableOpen(true)} disabled={!status?.enabled} variant="destructive" data-testid="admin-rum-disable-btn">
                    Disable RUM
                  </Button>
                </div>
              </>
            )}
          </div>
        </AnalyticsCard>
      </div>

      <EnableRumDialog service={currentService} open={isEnableOpen} onOpenChange={setIsEnableOpen} onComplete={async () => { await refetchStatus() }} />
      <DisableRumDialog service={currentService} open={isDisableOpen} onOpenChange={setIsDisableOpen} onComplete={async () => { await refetchStatus() }} />
    </div>
  )
}

export function RumNoDataBanner({ message, beaconCount }: { message?: string | null; beaconCount: number }) {
  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center bg-emerald-500/10 p-3 rounded-lg border border-emerald-500/20 text-emerald-900 dark:text-emerald-300 shadow-sm animate-in fade-in-50 duration-300">
        <div className="flex items-center gap-2">
          <span className="h-2 w-2 animate-pulse rounded-full bg-emerald-500" aria-hidden="true" />
          <span className="text-xs font-bold uppercase tracking-wider">Live RUM Stream Active</span>
        </div>
        <span className="text-xs opacity-85 font-medium">Waiting for real-time beacons…</span>
      </div>
      <AnalyticsCard title="Real User Monitoring Status">
        <div className="space-y-6 text-center py-12">
          <div>
            <p className="text-lg font-semibold text-muted-foreground mb-4">{message}</p>
            <div className="flex justify-center mb-6">
              <div className="w-48 h-2 bg-secondary rounded-full overflow-hidden">
                <div style={{ width: `${(beaconCount / 10) * 100}%` }} className="h-full bg-emerald-500 transition-all" />
              </div>
            </div>
            <p className="text-sm text-muted-foreground">
              Make sure the RUM script is installed on your website and users are visiting your site to generate beacons.
            </p>
          </div>
        </div>
      </AnalyticsCard>
    </div>
  )
}

export function RumEmptyWindowCard() {
  return (
    <div className="space-y-6">
      <AnalyticsCard title="Real User Monitoring Dashboard">
        <div className="space-y-6 text-center py-12">
          <div>
            <div className="mx-auto w-16 h-16 bg-muted/20 rounded-full flex items-center justify-center mb-4 border border-border">
              <Activity className="h-8 w-8 text-muted-foreground/50" />
            </div>
            <p className="text-lg font-semibold text-foreground">No data for this time period</p>
            <p className="text-sm text-muted-foreground mt-2 max-w-sm mx-auto">
              No real-time beacons were received during the selected time window. Try expanding your time range or check back later.
            </p>
          </div>
        </div>
      </AnalyticsCard>
    </div>
  )
}

export function RumInstallationDialog({
  open,
  onOpenChange,
  scriptTag = '<script src="/js/rum.js"></script>',
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  scriptTag?: string
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Installation Instructions</DialogTitle>
          <DialogDescription>Add Real User Monitoring to your website to start collecting performance data.</DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <h4 className="font-semibold text-sm">Step 1: Add the RUM Script</h4>
            <p className="text-sm text-muted-foreground">
              Add this single line to the `&lt;head&gt;` section of your HTML or any template that renders across your entire site. The version hash automatically updates when we release new RUM features:
            </p>
            <div className="bg-muted rounded-md overflow-hidden border">
              <div className="flex items-center justify-between px-3 py-2 border-b bg-muted/50">
                <span className="text-[10px] font-mono text-muted-foreground">installation code</span>
                <CopyButton text={scriptTag} ariaLabel="Copy script tag" copiedAriaLabel="Copied script tag" className="h-6 w-6 hover:bg-muted-foreground/10" />
              </div>
              <div className="p-3 font-mono text-xs overflow-x-auto"><code>{scriptTag}</code></div>
            </div>
          </div>

          <div className="space-y-2">
            <h4 className="font-semibold text-sm">Step 2: Works Everywhere</h4>
            <p className="text-sm text-muted-foreground">This script works on any website regardless of framework:</p>
            <ul className="text-sm text-muted-foreground list-disc list-inside space-y-1">
              <li>Plain HTML, PHP, Django, Rails, etc.</li>
              <li>React, Vue, Svelte, Angular, Next.js</li>
              <li>Static sites, dynamic apps, and everything in between</li>
            </ul>
          </div>

          <div className="space-y-2">
            <h4 className="font-semibold text-sm">Step 3: Start Collecting Data</h4>
            <p className="text-sm text-muted-foreground">
              Once deployed, real user beacons will automatically be sent to your analytics dashboard when users visit your site. After collecting 1+ beacons, you&apos;ll see real performance data appear on this page.
            </p>
          </div>

          <div className="bg-blue-50 dark:bg-blue-950 p-3 rounded-md space-y-2">
            <p className="text-sm font-semibold text-blue-900 dark:text-blue-100">💡 Tips:</p>
            <ul className="text-xs text-blue-800 dark:text-blue-200 space-y-1">
              <li>• Beacons capture Web Vitals, JavaScript errors, and session info automatically</li>
              <li>• The script is lightweight and asynchronous — no performance impact</li>
              <li>• Data is sent securely to your Fastly-backed analytics backend</li>
              <li>• The version hash automatically updates when we release RUM improvements — just copy the script tag again from these instructions</li>
            </ul>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  )
}
