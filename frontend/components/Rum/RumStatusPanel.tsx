'use client';

import { useState, useEffect } from 'react';
import { useSearchParams } from 'next/navigation';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { AlertTriangle, RefreshCw, TriangleAlert } from 'lucide-react';
import { AnalyticsCard } from '@/components/AnalyticsCard';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { useServiceStore } from '@/stores/filterStore';
import { useIsAnalyst } from '@/hooks/useBootstrap';
import { adminJsonResult, client } from '@/lib/api';
import { unwrapClient } from '@/lib/api';
import { queryKeys } from '@/lib/query-keys';
import { cn } from '@/lib/utils';
import type { components } from '@/types/api.generated';
import { DisableRumDialog, EnableRumDialog, RumSettingsDialog } from './RumSettingsDialog';
import { useRumStatus } from '@/hooks/useDashboardCards';
import { formatLocalDateTime } from '@/lib/format';

type RumVersionsResponse = components['schemas']['RumVersionsResponse'];

interface RumFaroVersionCardProps {
  serviceId: string | null;
}

export function RumFaroVersionCard({ serviceId }: RumFaroVersionCardProps) {
  const queryClient = useQueryClient();
  const [isUpgradeOpen, setIsUpgradeOpen] = useState(false);
  const isAnalyst = useIsAnalyst();

  // Fetch status first
  const { data: status, isLoading: isStatusLoading } = useRumStatus(serviceId);

  const rumEnabled = !!status?.enabled;

  // Fetch versions
  const { data, isLoading: isVersionsLoading, isError, refetch, isFetching } = useQuery<RumVersionsResponse>({
    queryKey: queryKeys.rumVersions(serviceId),
    queryFn: async ({ signal }) =>
      unwrapClient(
        client.GET('/api/services/{service_id}/rum/versions', {
          params: { path: { service_id: serviceId as string } },
          signal,
        }),
      ),
    enabled: !!serviceId && rumEnabled && !isAnalyst,
    retry: false,
  });

  // Analysts do not see version management controls in the header
  if (isAnalyst) {
    return null;
  }

  if (!serviceId || isStatusLoading || (rumEnabled && isVersionsLoading)) {
    return (
      <div className="flex items-center gap-2 shrink-0">
        <Skeleton className="h-8 w-32" />
        <Skeleton className="h-8 w-24" />
      </div>
    );
  }

  // Not enabled: nothing to pin/upgrade
  if (!rumEnabled) {
    return null;
  }

  if (isError) {
    return (
      <div className="flex items-center gap-2 text-xs text-destructive bg-destructive/10 px-2.5 py-1 rounded-md border border-destructive/20 shrink-0">
        <TriangleAlert className="h-3.5 w-3.5 shrink-0" />
        <span>Versions offline</span>
        <Button
          variant="ghost"
          size="icon"
          onClick={() => refetch()}
          disabled={isFetching}
          aria-label="Retry"
          className="h-5 w-5 hover:bg-destructive/20 hover:text-destructive shrink-0"
        >
          <RefreshCw className={cn('h-2.5 w-2.5', isFetching && 'animate-spin')} />
        </Button>
      </div>
    );
  }

  const available = data?.available ?? [];
  const current = data?.current ?? null;
  const latest = data?.latest ?? null;
  const updateAvailable = data?.update_available ?? false;

  const handleComplete = () => {
    queryClient.invalidateQueries({ queryKey: queryKeys.rumVersions(serviceId) });
    queryClient.invalidateQueries({ queryKey: queryKeys.rumStatus(serviceId) });
  };

  return (
    <>
      <div className="flex items-center gap-2 shrink-0" data-testid="admin-rum-faro-version-card">
        <div className="flex items-center gap-1.5 text-xs text-muted-foreground mr-1">
          <span className="font-semibold uppercase tracking-wider text-[10px]">Faro SDK:</span>
          <span
            className="font-mono font-medium text-foreground bg-muted/60 px-1.5 py-0.5 rounded border border-muted-foreground/10"
            data-testid="admin-rum-faro-version"
          >
            {current ?? 'Not pinned'}
          </span>
          {updateAvailable && (
            <Badge variant="warning" className="text-[9px] py-0 px-1.5 h-4.5 font-medium ml-1 shrink-0">
              Update available
            </Badge>
          )}
        </div>

        <Button
          onClick={() => setIsUpgradeOpen(true)}
          variant={updateAvailable ? 'default' : 'outline'}
          size="sm"
          className="h-8 text-xs font-semibold px-3"
          data-testid="admin-rum-settings-btn"
        >
          RUM Settings
        </Button>
      </div>

      <RumSettingsDialog
        serviceId={serviceId}
        open={isUpgradeOpen}
        onOpenChange={setIsUpgradeOpen}
        availableVersions={available}
        currentVersion={current}
        latestVersion={latest}
        onComplete={handleComplete}
      />
    </>
  );
}

export function RumStatusPanel() {
  const searchParams = useSearchParams();
  const queryClient = useQueryClient();
  const queryServiceId = searchParams?.get('service');
  const storeServiceId = useServiceStore((s) => s.activeServiceId);
  const setActiveServiceId = useServiceStore((s) => s.setActiveServiceId);

  useEffect(() => {
    if (queryServiceId && queryServiceId !== storeServiceId) {
      setActiveServiceId(queryServiceId);
    }
  }, [queryServiceId, storeServiceId, setActiveServiceId]);

  const {
    data: services,
    isLoading: isServicesLoading,
    refetch: refetchServices,
  } = useQuery({
    queryKey: queryKeys.services(),
    queryFn: async () => {
      const res = await adminJsonResult('/api/services', '/api/services');
      return res.ok ? res.data : null;
    },
  });

  const serviceId =
    queryServiceId ||
    storeServiceId ||
    services?.services?.[0]?.service_id ||
    null;
  const isAnalyst = useIsAnalyst();
  const [isEnableOpen, setIsEnableOpen] = useState(false);
  const [isDisableOpen, setIsDisableOpen] = useState(false);

  const {
    data: status,
    isLoading: isStatusLoading,
    isFetching: isStatusFetching,
    refetch: refetchStatus,
  } = useRumStatus(serviceId, { refetchInterval: 30_000 }); // Poll every 30s

  const currentService =
    services?.services?.find(
      (s: components['schemas']['ServiceConfig']) => s.service_id === serviceId
    ) || null;

  const isInitialLoading =
    (!serviceId && isServicesLoading) || (!!serviceId && isStatusLoading && !status);
  const isRefreshing = isStatusFetching && !isInitialLoading;

  const handleManualRefresh = async () => {
    await Promise.all([
      refetchStatus(),
      refetchServices(),
      serviceId
        ? queryClient.invalidateQueries({ queryKey: ['rum-versions', serviceId] })
        : Promise.resolve(),
    ]);
  };

  if (isAnalyst && status?.unauthorized) {
    return (
      <AnalyticsCard title="RUM Status">
        <div className="flex flex-col items-center justify-center gap-4 py-16 text-center">
          <AlertTriangle className="h-12 w-12 text-destructive" />
          <div>
            <h3 className="text-lg font-medium text-destructive">Access Denied</h3>
            <p className="text-sm text-muted-foreground mt-1">
              You are not authorized to view RUM data for this service. Please contact your administrator to grant access.
            </p>
          </div>
        </div>
      </AnalyticsCard>
    );
  }

  // Analysts can't reach /rum/enable or /rum/disable — the middleware
  // blocks both (mutate deployed edge config) — so showing this panel's
  // controls when RUM isn't enabled would just 403 on click. Mirrors the
  // "feature not enabled for this service" empty state StreamingClient.tsx
  // uses when CMCD isn't on for a service.
  if (isAnalyst && !isInitialLoading && !status?.enabled) {
    return (
      <AnalyticsCard title="RUM Status">
        <div className="flex flex-col items-center justify-center gap-4 py-16 text-center">
          <AlertTriangle className="h-12 w-12 text-muted-foreground" />
          <div>
            <h3 className="text-lg font-medium">RUM not enabled</h3>
            <p className="text-sm text-muted-foreground mt-1">
              Real User Monitoring is not enabled for this service. Ask an administrator to enable it.
            </p>
          </div>
        </div>
      </AnalyticsCard>
    );
  }

  const enabledCollectors = status
    ? [
        status.capture_vitals !== false && 'Vitals',
        status.capture_performance !== false && 'Performance',
        status.capture_errors !== false && 'Errors',
        status.capture_events !== false && 'Events',
      ].filter(Boolean)
    : [];

  return (
    <div className="space-y-4" data-testid="admin-rum-panel">
      {/* Toolbar: status badge + Faro SDK / Settings controls + manual Refresh */}
      <div
        className="flex items-center justify-between gap-3 rounded-lg border bg-card px-4 py-3 h-[56px] min-h-[56px] [contain-intrinsic-size:auto_56px] [content-visibility:auto]"
        data-testid="admin-rum-toolbar"
      >
        <div className="flex items-center gap-3 min-w-0">
          <Badge
            variant={
              isInitialLoading
                ? 'secondary'
                : status?.enabled
                  ? 'default'
                  : 'outline'
            }
            className="min-w-[152px] justify-center shrink-0"
            data-testid="admin-rum-status-badge"
          >
            {isInitialLoading
              ? 'Checking...'
              : status?.enabled
                ? 'Beacon Pipeline Active'
                : 'Beacon Pipeline Inactive'}
          </Badge>
          <span
            className="text-xs text-muted-foreground truncate"
            data-testid="admin-rum-service-label"
          >
            Service:{' '}
            <strong className="text-foreground">
              {currentService?.name ?? serviceId ?? 'Loading...'}
            </strong>
          </span>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          <RumFaroVersionCard serviceId={serviceId} />
          <Button
            variant="outline"
            size="sm"
            onClick={handleManualRefresh}
            disabled={isStatusFetching || !serviceId}
            data-testid="admin-rum-refresh-btn"
            className="h-8 text-xs shrink-0"
          >
            <RefreshCw
              className={cn('mr-2 h-3.5 w-3.5', isStatusFetching && 'animate-spin')}
            />
            Refresh
          </Button>
        </div>
      </div>

      {/* Zero-CLS pre-allocated reservation container for the RUM Status card */}
      <div
        className="min-h-[260px] [contain-intrinsic-size:auto_260px] [content-visibility:auto]"
        data-testid="admin-rum-status-reservation"
      >
        <AnalyticsCard
          title="RUM Status"
          className={cn(
            'space-y-4 min-h-[260px]',
            isRefreshing && 'opacity-40 pointer-events-none transition-opacity'
          )}
        >
          <div data-testid="admin-rum-status-card" className="space-y-4">
            {isInitialLoading ? (
              <div className="space-y-4" data-testid="admin-rum-status-skeleton">
                <div className="space-y-2 min-h-[112px]">
                  <p className="text-xs text-muted-foreground">
                    Inspecting RUM beacon pipeline...
                  </p>
                  <Skeleton className="h-4 w-48" />
                  <Skeleton className="h-4 w-64" />
                  <Skeleton className="h-4 w-40" />
                </div>
                <div className="flex gap-2">
                  <Skeleton className="h-9 w-28" />
                  <Skeleton className="h-9 w-28" />
                </div>
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
                        <strong>Enabled at:</strong>{' '}
                        {status.enabled_at ? formatLocalDateTime(status.enabled_at) : 'Unknown'}
                      </p>
                      <p className="text-sm" data-testid="admin-rum-vcl-drift">
                        <strong>VCL Drift:</strong> {status.vcl_drift ? '⚠️ Yes' : '✅ No'}
                      </p>
                      {!isAnalyst && (
                        <p className="text-sm" data-testid="admin-rum-capture-summary">
                          <strong>Collectors:</strong>{' '}
                          {enabledCollectors.length > 0
                            ? enabledCollectors.join(', ')
                            : 'None'}
                          {status.custom_condition
                            ? ` · VCL Condition: ${status.custom_condition}`
                            : ''}
                        </p>
                      )}
                    </>
                  )}
                </div>

                <div className="flex gap-2">
                  <Button
                    onClick={() => setIsEnableOpen(true)}
                    disabled={status?.enabled}
                    variant={status?.enabled ? 'secondary' : 'default'}
                    data-testid="admin-rum-enable-btn"
                  >
                    Enable RUM
                  </Button>
                  <Button
                    onClick={() => setIsDisableOpen(true)}
                    disabled={!status?.enabled}
                    variant="destructive"
                    data-testid="admin-rum-disable-btn"
                  >
                    Disable RUM
                  </Button>
                </div>
              </>
            )}
          </div>
        </AnalyticsCard>
      </div>

      <EnableRumDialog
        service={currentService}
        open={isEnableOpen}
        onOpenChange={setIsEnableOpen}
        onComplete={async () => {
          await refetchStatus();
        }}
      />

      <DisableRumDialog
        service={currentService}
        open={isDisableOpen}
        onOpenChange={setIsDisableOpen}
        onComplete={async () => {
          await refetchStatus();
        }}
      />
    </div>
  );
}
