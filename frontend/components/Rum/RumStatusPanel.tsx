'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { AlertTriangle } from 'lucide-react';
import { AnalyticsCard } from '@/components/AnalyticsCard';
import { Button } from '@/components/ui/button';
import { useServiceStore } from '@/stores/serviceStore';
import { useIsAnalyst } from '@/hooks/useIsAnalyst';
import { adminFetch } from '@/lib/api';
import type { components } from '@/types/api.generated';
import { DisableRumDialog } from './DisableRumDialog';
import { EnableRumDialog } from './EnableRumDialog';


interface RumStatus {
  enabled: boolean;
  enabled_at?: string | null;
  deployed_vcl_sha?: string | null;
  current_vcl_sha?: string;
  vcl_drift?: boolean;
  unauthorized?: boolean;
}

export function RumStatusPanel() {
  const serviceId = useServiceStore((s) => s.activeServiceId);
  const isAnalyst = useIsAnalyst();
  const [isEnableOpen, setIsEnableOpen] = useState(false);
  const [isDisableOpen, setIsDisableOpen] = useState(false);

  const { data: status, refetch: refetchStatus } = useQuery<RumStatus | null>({
    queryKey: ['rum-status', serviceId],
    queryFn: async () => {
      if (!serviceId) return null;
      const res = await adminFetch(`/api/services/${serviceId}/rum/status`);
      if (res.status === 403) {
        return { enabled: false, unauthorized: true };
      }
      if (!res.ok) return null;
      return res.json() as Promise<RumStatus>;
    },
    enabled: !!serviceId,
    refetchInterval: 30_000, // Poll every 30s
  });

  const { data: services } = useQuery({
    queryKey: ['services'],
    queryFn: async () => {
      const res = await adminFetch('/api/services');
      if (!res.ok) return null;
      return res.json();
    },
  });

  const currentService = services?.services?.find(
    (s: components['schemas']['ServiceConfig']) => s.service_id === serviceId
  ) || null;

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
  if (isAnalyst && !status?.enabled) {
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

  return (
    <>
      <AnalyticsCard title="RUM Status" className="space-y-4">
        <div className="space-y-2">
          <p className="text-sm">
            <strong>Status:</strong> {status?.enabled ? '✅ Enabled' : '❌ Disabled'}
          </p>
          {status?.enabled && (
            <>
              <p className="text-sm">
                <strong>Enabled at:</strong> {status.enabled_at ? new Date(status.enabled_at).toLocaleString() : 'Unknown'}
              </p>
              <p className="text-sm">
                <strong>VCL Drift:</strong> {status.vcl_drift ? '⚠️ Yes' : '✅ No'}
              </p>
            </>
          )}
        </div>

        <div className="flex gap-2">
          <Button
            onClick={() => setIsEnableOpen(true)}
            disabled={status?.enabled}
            variant={status?.enabled ? 'secondary' : 'default'}
          >
            Enable RUM
          </Button>
          <Button
            onClick={() => setIsDisableOpen(true)}
            disabled={!status?.enabled}
            variant="destructive"
          >
            Disable RUM
          </Button>
        </div>
      </AnalyticsCard>



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
    </>
  );
}
