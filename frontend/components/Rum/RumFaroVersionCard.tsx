'use client';

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { RefreshCw, TriangleAlert } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { cn } from '@/lib/utils';
import { client } from '@/lib/api';
import { useIsAnalyst } from '@/hooks/useIsAnalyst';
import type { components } from '@/types/api.generated';
import { RumSettingsDialog } from './RumSettingsDialog';
import { useRumStatus } from '@/hooks/useRumStatus';
import { queryKeys } from '@/lib/query-keys';

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
    queryKey: ['rum-versions', serviceId],
    queryFn: async ({ signal }) => {
      const { data, response } = await client.GET('/api/services/{service_id}/rum/versions', {
        params: { path: { service_id: serviceId as string } },
        signal,
      });
      if (!response.ok) throw new Error(`status ${response.status}`);
      return data as RumVersionsResponse;
    },
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
    queryClient.invalidateQueries({ queryKey: ['rum-versions', serviceId] });
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
