/**
 * RUM Admin Configuration Page
 * Mirrors session_scoring/page.tsx and admin/queue/page.tsx patterns:
 * - Enable/disable RUM via SSE modal
 * - Status display (enabled, drift, capture settings, Faro SDK version)
 * - Zero-CLS pre-allocated layout reservations & manual refresh
 * - Admin-only access
 */

import { Suspense } from 'react';
import { Metadata } from 'next';
import { BackToAdminLink } from '@/components/ReportLayout';
import { PageHeader } from '@/components/ui/confirm-dialog';
import { RumStatusPanel } from '@/components/Rum/RumStatusPanel';
import { RumPanelFallback } from './loading';

export const metadata: Metadata = {
  title: 'RUM Configuration',
};

export default function RumAdminPage() {
  return (
    <div className="space-y-6" data-testid="admin-rum-root">
      <PageHeader
        title="Real User Monitoring"
        description="Configure and monitor RUM collection for your service"
      >
        <BackToAdminLink />
      </PageHeader>

      <Suspense fallback={<RumPanelFallback />}>
        <RumStatusPanel />
      </Suspense>
    </div>
  );
}
