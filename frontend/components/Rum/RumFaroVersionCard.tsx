'use client';

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { RefreshCw, TriangleAlert, ArrowUpCircle } from 'lucide-react';
import { AnalyticsCard } from '@/components/AnalyticsCard';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { cn } from '@/lib/utils';
import { client } from '@/lib/api';
import type { components } from '@/types/api.generated';
import { UpgradeFaroDialog } from './UpgradeFaroDialog';

type RumVersionsResponse = components['schemas']['RumVersionsResponse'];

interface RumFaroVersionCardProps {
  serviceId: string | null;
  rumEnabled: boolean;
}

/**
 * Surfaces the pinned vs latest Grafana Faro Web SDK version and lets the
 * operator pick a target to upgrade to (Task 8 — the surface the user asked
 * for: "if there is a new version we should let the user know on the RUM
 * page and ask them if they want to upgrade").
 *
 * Deliberately a one-shot fetch, not polled: the npm-registry-backed
 * /rum/versions lookup is only interesting right after a page load or an
 * explicit refresh — see the precedent at SessionScoring/StatusPanel.tsx
 * (polling an admin status surface previously caused ~1.5 GB of
 * `.duckdb-wal` churn).
 */
export function RumFaroVersionCard({ serviceId, rumEnabled }: RumFaroVersionCardProps) {
  const queryClient = useQueryClient();
  const [isUpgradeOpen, setIsUpgradeOpen] = useState(false);

  const { data, isLoading, isError, refetch, isFetching } = useQuery<RumVersionsResponse>({
    queryKey: ['rum-versions', serviceId],
    queryFn: async ({ signal }) => {
      const { data, response } = await client.GET('/api/services/{service_id}/rum/versions', {
        params: { path: { service_id: serviceId as string } },
        signal,
      });
      if (!response.ok) throw new Error(`status ${response.status}`);
      return data as RumVersionsResponse;
    },
    enabled: !!serviceId && rumEnabled,
    retry: false,
  });

  // Not enabled: nothing to pin/upgrade, and the operator hasn't asked to
  // provision RUM yet — don't invite an upgrade action for a feature that
  // isn't on, and skip the registry fetch entirely.
  if (!rumEnabled) {
    return (
      <AnalyticsCard title="Faro SDK Version">
        <p className="text-sm text-muted-foreground">
          Enable RUM above to pin and upgrade the self-hosted Faro Web SDK bundle.
        </p>
      </AnalyticsCard>
    );
  }

  if (isLoading) {
    return (
      <AnalyticsCard title="Faro SDK Version">
        <Skeleton className="h-16 w-full" />
      </AnalyticsCard>
    );
  }

  if (isError) {
    return (
      <AnalyticsCard title="Faro SDK Version">
        <Alert variant="destructive">
          <TriangleAlert className="h-4 w-4" />
          <AlertTitle>Couldn&apos;t reach the npm registry</AlertTitle>
          <AlertDescription className="flex items-center justify-between gap-2">
            <span>Version info is temporarily unavailable. The rest of the RUM admin surface is unaffected.</span>
            <Button
              variant="outline"
              size="sm"
              onClick={() => refetch()}
              disabled={isFetching}
              className="h-7 text-xs shrink-0"
            >
              <RefreshCw className={cn('h-3 w-3 mr-1', isFetching && 'animate-spin')} />
              {isFetching ? 'Retrying…' : 'Retry'}
            </Button>
          </AlertDescription>
        </Alert>
      </AnalyticsCard>
    );
  }

  const available = data?.available ?? [];
  const current = data?.current ?? null;
  const latest = data?.latest ?? null;
  const updateAvailable = data?.update_available ?? false;
  const noVersions = available.length === 0;

  const handleComplete = () => {
    queryClient.invalidateQueries({ queryKey: ['rum-versions', serviceId] });
    queryClient.invalidateQueries({ queryKey: ['rum-status', serviceId] });
  };

  return (
    <>
      <AnalyticsCard
        title="Faro SDK Version"
        description="The self-hosted Grafana Faro Web SDK bundle served from Object Storage."
        headerAction={
          <Button
            variant="ghost"
            size="icon"
            onClick={() => refetch()}
            disabled={isFetching}
            aria-label="Refresh version info"
            className="h-7 w-7"
          >
            <RefreshCw className={cn('h-3.5 w-3.5', isFetching && 'animate-spin')} />
          </Button>
        }
      >
        {noVersions ? (
          <p className="text-sm text-muted-foreground">No versions available from the registry right now.</p>
        ) : (
          <div className="space-y-4">
            <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm">
              <div>
                <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Pinned</p>
                <p className="font-mono">{current ?? 'Not pinned'}</p>
              </div>
              <div>
                <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Latest</p>
                <p className="font-mono">{latest ?? '—'}</p>
              </div>
              {updateAvailable ? (
                <Badge variant="warning">Update available</Badge>
              ) : current ? (
                <Badge variant="success">Up to date</Badge>
              ) : null}
            </div>

            {updateAvailable && (
              <Alert className="border-amber-300 bg-amber-50/60 dark:bg-amber-950/20 text-amber-900 dark:text-amber-300">
                <ArrowUpCircle className="h-4 w-4" />
                <AlertTitle className="text-sm font-bold">New Faro Web SDK version available</AlertTitle>
                <AlertDescription className="text-[13px]">
                  v{latest} is available (currently pinned to v{current}).
                </AlertDescription>
              </Alert>
            )}

            <div className="flex justify-end">
              <Button
                onClick={() => setIsUpgradeOpen(true)}
                disabled={!updateAvailable && !!current}
                variant={updateAvailable ? 'default' : 'outline'}
                size="sm"
              >
                {current ? (updateAvailable ? 'Upgrade' : 'Up to date') : 'Choose version'}
              </Button>
            </div>
          </div>
        )}
      </AnalyticsCard>

      <UpgradeFaroDialog
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
